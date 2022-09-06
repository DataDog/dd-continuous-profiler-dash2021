package movies;

import static spark.Spark.exception;
import static spark.Spark.get;
import static spark.Spark.port;

import java.io.InputStreamReader;
import java.io.IOException;
import java.time.LocalDate;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import java.util.zip.GZIPInputStream;

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;

import com.mongodb.client.MongoClients;
import org.bson.Document;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import spark.Request;
import spark.Response;

public class Server {
	private static final Gson GSON = new GsonBuilder().setLenient().setPrettyPrinting().create();
	private static final Logger LOG = LoggerFactory.getLogger(Server.class);

	private static final Supplier<List<Movie>> MOVIES = Suppliers.memoize(Server::loadMovies);
	private static final Supplier<List<Credit>> CREDITS = Server::loadCredits;

	public static void main(String[] args) {
		port(8081);
		get("/", Server::randomMovieEndpoint);
		get("/credits", Server::creditsEndpoint);
		get("/movies", Server::moviesEndpoint);
		get("/old-movies", Server::oldMoviesEndpoint);
		get("/stats", Server::statsEndpoint);

		// Warm these up at application start
		MOVIES.get();
		CREDITS.get();

		exception(Exception.class, (exception, request, response) -> exception.printStackTrace());

		var version = System.getProperty("dd.version");
		LOG.info("Running version " + (version != null ? version.toLowerCase() : "(not set)") + " with pid " + ProcessHandle.current().pid());
	}

	private static Object randomMovieEndpoint(Request req, Response res) {
		return replyJSON(res, MOVIES.get().get(new Random().nextInt(MOVIES.get().size())));
	}

	private static Object creditsEndpoint(Request req, Response res) {
		var movies = MOVIES.get().stream();
		var query = req.queryParamOrDefault("q", req.queryParams("query"));

		if (query != null) {
			var p = Pattern.compile(query, Pattern.CASE_INSENSITIVE);
			movies = movies.filter(m -> m.title != null && p.matcher(m.title).find());
		}

		var moviesWithCredits = movies.map(movie -> new MovieWithCredits(movie, creditsForMovie(movie)));
		return replyJSON(res, moviesWithCredits);
	}

	private static Object statsEndpoint(Request req, Response res) {
		var movies = MOVIES.get().stream();
		var query = req.queryParamOrDefault("q", req.queryParams("query"));

		if (query != null) {
			var p = Pattern.compile(query, Pattern.CASE_INSENSITIVE);
			movies = movies.filter(m -> m.title != null && p.matcher(m.title).find());
		}

		var selectedMovies = movies.toList();

		var numberMatched = selectedMovies.size();
		var statsForMovies = selectedMovies.stream().map(movie -> crewCountForMovie(creditsForMovie(movie)));
		var aggregatedStats =
			statsForMovies
				.flatMap(countMap -> countMap.entrySet().stream())
				.collect(Collectors.groupingBy(Map.Entry::getKey, Collectors.summingLong(Map.Entry::getValue)));

		return replyJSON(res, new StatsResult(numberMatched, aggregatedStats));
	}

	private static List<Credit> creditsForMovie(Movie movie) {
		return CREDITS.get().stream().filter(c -> c.id.equals(movie.id)).toList();
	}

	private static Map<CrewRole, Long> crewCountForMovie(List<Credit> credits) {
		var credit = credits != null ? credits.get(0) : null;
		return credit != null ?
			credit.crewRole.stream().collect(Collectors.groupingBy(Server::parseRole, Collectors.counting())) : Map.of();
	}

	private static CrewRole parseRole(String role) {
		try {
			return CrewRole.valueOf(role);
		} catch (IllegalArgumentException e) {
			LOG.trace("Unknown role", e);
			return CrewRole.Other;
		}
	}

	private static Object moviesEndpoint(Request req, Response res) {
		var movies = MOVIES.get().stream();
		movies = sortByDescReleaseDate(movies);
		var query = req.queryParamOrDefault("q", req.queryParams("query"));
		if (query != null) {
			movies = movies.filter(m -> Pattern.matches(".*" + query.toUpperCase() + ".*", m.title.toUpperCase()));
		}
		return replyJSON(res, movies);
	}

	private static Stream<Movie> sortByDescReleaseDate(Stream<Movie> movies) {
		return movies.sorted(Comparator.comparing((Movie m) -> {
			try {
				return LocalDate.parse(m.releaseDate);
			} catch (Exception e) {
				return LocalDate.MIN;
			}
		}).reversed());
	}

	private static Object oldMoviesEndpoint(Request req, Response res) {
		var year = req.queryParamOrDefault("year", "2010");
		var limit = Integer.valueOf(req.queryParamOrDefault("n", "10"));

		var oldMovies = MOVIES.get().stream().filter(m -> isOlderThan(year, m)).toList();
		LOG.debug("Found the following oldMovies: " + oldMovies);
		oldMovies = oldMovies.stream().limit(limit).toList();
		LOG.debug("With limit " + limit + ", the result was: " + oldMovies);

		return replyJSON(res, oldMovies);
	}

	private static boolean isOlderThan(String year, Movie movie) {
		var result = movie.releaseDate.compareTo(year) < 0;
		LOG.debug("Is " + movie + " older than " + year + "? " + result);
		return result;
	}

	private static Object replyJSON(Response res, Stream<?> data) { return replyJSON(res, data.toList()); }
	private static Object replyJSON(Response res, Object data) {
		res.type("application/json");
		return GSON.toJson(data);
	}

	private static List<Movie> loadMovies() {
		try (
			var is = ClassLoader.getSystemResourceAsStream("movies-v2.json.gz");
			var gzis = new GZIPInputStream(is);
			var reader = new InputStreamReader(gzis)
		) {
			return GSON.<List<Movie>>fromJson(reader, new TypeToken<List<Movie>>() {}.getType())
				.stream().map(Movie::cacheLowerCaseTitle).toList();
		} catch (IOException e) {
			throw new RuntimeException("Failed to load movie data", e);
		}
	}

	private static List<Credit> loadCredits() {
		try (
			var mongoClient = MongoClients.create()
		) {
			var creditsCollection = mongoClient.getDatabase("moviesDB").getCollection("credits");
			return StreamSupport.stream(creditsCollection.find().batchSize(5_000).map(Credit::new).spliterator(), false).toList();
		}
	}

	public static class Movie {
		String id;
		String originalTitle;
		String overview;
		String releaseDate;
		String tagline;
		String title;
		String voteAverage;
		transient String lowerCaseTitle;

		Movie cacheLowerCaseTitle() {
			this.lowerCaseTitle = title.toLowerCase();
			return this;
		}

		public String toString() { return GSON.toJson(this).toString(); }
	}

	public static class Credit {
		String id;
		List<String> crew;
		List<String> cast;
		transient List<String> crewRole;

		private static final Pattern ROLE = Pattern.compile("\\((.*)\\)");

		public Credit(Document data) {
			this.id = data.getString("id");
			this.crew = data.getList("crew", String.class);
			this.cast = data.getList("cast", String.class);
			this.crewRole = data.getList("crew", String.class).stream().map(Credit::getRole).toList();
		}

		private static String getRole(String nameAndRole) {
			var matcher = ROLE.matcher(nameAndRole);
			matcher.find();
			return matcher.group(1);
		}
	}
	public record MovieWithCredits(Movie movie, List<Credit> credits) { }

	public enum CrewRole {
		Director, Writer, Screenplay, Editor, Animation, Other;

		public static Map<String, CrewRole> ROLES_MAP =
			Arrays.stream(CrewRole.class.getEnumConstants()).collect(Collectors.toMap(CrewRole::toString, Function.identity()));
	}
	public record StatsResult(int matchedMovies, Map<CrewRole, Long> crewCount) { }
}
