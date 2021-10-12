package movies;

import static spark.Spark.exception;
import static spark.Spark.get;
import static spark.Spark.port;

import java.io.IOException;
import java.io.InputStreamReader;
import java.time.LocalDate;
import java.util.Comparator;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import java.util.zip.GZIPInputStream;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.annotations.SerializedName;
import com.google.gson.reflect.TypeToken;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;

import com.mongodb.client.MongoClients;
import org.bson.Document;

import spark.Request;
import spark.Response;

public class Server {
	private static final Gson GSON;
	private static final Supplier<List<Movie>> MOVIES;
	private static final Supplier<List<Credit>> CREDITS;

	static {
		GSON = new GsonBuilder().setLenient().setPrettyPrinting().create();
		MOVIES = Suppliers.memoize(Server::loadMovies);
		CREDITS = Server::loadCredits;
	}

	public static void main(String[] args) {
		port(8081);
		get("/credits", Server::creditsEndpoint);
		get("/movies", Server::moviesEndpoint);

		MOVIES.get();
		CREDITS.get();

		exception(Exception.class, (exception, request, response) -> {
			System.err.println(exception.getMessage());
			exception.printStackTrace();
		});
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
		// Example Solution: Memoize the CREDITS supplier.
		var credits = CREDITS.get();

		// Problem: We are doing a O(n^2) search.
		// Example Solution: Use a map with O(1) access time, reducing the overall complexity to O(n)
		//   return CREDITS_BY_MOVIEID.get().get(movie.id);
		return credits.stream().filter(c -> c.id.equals(movie.id)).collect(Collectors.toList());
	}

	private static Object moviesEndpoint(Request req, Response res) {
		var movies = MOVIES.get().stream();
		movies = sortByDescReleaseDate(movies);
		var query = req.queryParamOrDefault("q", req.queryParams("query"));
		if (query != null) {
			// Problem: We are not compiling the pattern and there's a more efficient way of ignoring cases.
			// Solution:
			//   var p = Pattern.compile(query, Pattern.CASE_INSENSITIVE);
			//   movies = movies.filter(m -> p.matcher(m.title).find());
			movies = movies.filter(m -> Pattern.matches(".*" + query.toUpperCase() + ".*", m.title.toUpperCase()));
		}
		return replyJSON(res, movies);
	}

	private static Stream<Movie> sortByDescReleaseDate(Stream<Movie> movies) {
		return movies.sorted(Comparator.comparing((Movie m) -> {
			// Problem: We are parsing a datetime for each item to be sorted.
			// Example Solution:
			//   Since date is in isoformat (yyyy-mm-dd) already, that one sorts nicely with normal string sorting
			//   `return m.releaseDate`
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
