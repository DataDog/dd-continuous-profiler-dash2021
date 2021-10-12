package movies;

import static spark.Spark.exception;
import static spark.Spark.get;
import static spark.Spark.port;

import java.io.InputStreamReader;
import java.io.IOException;
import java.time.LocalDate;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import java.util.zip.GZIPInputStream;

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.gson.annotations.SerializedName;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;

import com.mongodb.client.MongoClients;
import org.bson.Document;

import spark.Request;
import spark.Response;

public class Server {
	private static final Gson GSON = new GsonBuilder().setLenient().setPrettyPrinting().create();
	private static final Supplier<List<Movie>> MOVIES = Suppliers.memoize(Server::loadMovies);
	private static final Supplier<List<Credit>> CREDITS = Server::loadCredits;
	// Placeholder for future improvement

	public static void main(String[] args) {
		port(8081);
		get("/", Server::randomMovieEndpoint);
		get("/credits", Server::creditsEndpoint);
		get("/movies", Server::moviesEndpoint);

		MOVIES.get();
		CREDITS.get();

		exception(Exception.class, (exception, request, response) -> {
			System.err.println(exception.getMessage());
			exception.printStackTrace();
		});
	}

	private static Object randomMovieEndpoint(Request req, Response res) {
		var randomMovie = MOVIES.get().get(new Random().nextInt(MOVIES.get().size()));
		return replyJSON(res, randomMovie);
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

	private static List<Credit> creditsForMovie(Movie movie) {
		// Problem: We are loading the credits every time this method gets called.
		// Problem: We are searching the entire credits list for every single movie.
		return CREDITS.get().stream().filter(c -> c.id.equals(movie.id)).collect(Collectors.toList());
	}

	private static Object moviesEndpoint(Request req, Response res) {
		var movies = MOVIES.get().stream();
		movies = sortByDescReleaseDate(movies);
		var query = req.queryParamOrDefault("q", req.queryParams("query"));
		if (query != null) {
			// Problem: We are not compiling the pattern and there's a more efficient way of ignoring cases.
			movies = movies.filter(m -> Pattern.matches(".*" + query.toUpperCase() + ".*", m.title.toUpperCase()));
		}
		return replyJSON(res, movies);
	}

	private static Stream<Movie> sortByDescReleaseDate(Stream<Movie> movies) {
		return movies.sorted(Comparator.comparing((Movie m) -> {
			// Problem: We are parsing a datetime for each item to be sorted.
			try {
				return LocalDate.parse(m.releaseDate);
			} catch (Exception e) {
				return LocalDate.MIN;
			}
		}).reversed());
	}

	private static Object replyJSON(Response res, Stream<?> data) {
		return replyJSON(res, data.collect(Collectors.toList()));
	}

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
			return GSON.fromJson(reader, new TypeToken<List<Movie>>() {}.getType());
		} catch (IOException e) {
			throw new RuntimeException("Failed to load movie data");
		}
	}

	private static List<Credit> loadCredits() {
		try (
			var mongoClient = MongoClients.create()
		) {
			var creditsCollection = mongoClient.getDatabase("moviesDB").getCollection("credits");
			return StreamSupport
				.stream(creditsCollection.find().batchSize(5_000).map(Credit::new).spliterator(), false)
				.collect(Collectors.toList());
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
	}

	public static record Credit(String id, List<String> crew, List<String> cast) {
		public Credit(Document data) {
			this(data.getString("id"), data.getList("crew", String.class), data.getList("cast", String.class));
		}
	}
	public static record MovieWithCredits(Movie movie, List<Credit> credits) { }
}
