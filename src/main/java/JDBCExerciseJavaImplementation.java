import de.hpi.dbs1.ChosenImplementation;
import de.hpi.dbs1.ConnectionConfig;
import de.hpi.dbs1.JDBCExercise;
import de.hpi.dbs1.entities.Actor;
import de.hpi.dbs1.entities.Movie;
import org.jetbrains.annotations.NotNull;

import java.sql.*;
import java.util.*;
import java.util.logging.Logger;

@ChosenImplementation(true)
public class JDBCExerciseJavaImplementation implements JDBCExercise {

	Logger logger = Logger.getLogger(this.getClass().getSimpleName());

	@Override
	public Connection createConnection(@NotNull ConnectionConfig config) throws SQLException {
		String url = "jdbc:postgresql://" + config.getHost() + ":" + config.getPort() + "/" + config.getDatabase();
		return DriverManager.getConnection(url, config.getUsername(), config.getPassword());
	}

	@Override
	public List<Movie> queryMovies(
		@NotNull Connection connection,
		@NotNull String keywords
	) throws SQLException {
		logger.info(keywords);
		List<Movie> movies = new ArrayList<>();
		PreparedStatement movies_statement = connection.prepareStatement("SELECT * FROM tmovies WHERE \"primaryTitle\" LIKE ? ORDER BY \"primaryTitle\", \"startYear\"");
		PreparedStatement actors_stament = connection.prepareStatement("SELECT primaryname FROM tprincipals NATURAL JOIN nbasics WHERE tconst = ? AND (category='actor' OR category='actress') ORDER BY primaryname");
		movies_statement.setString(1, "%" + keywords + "%");
		ResultSet movie_results = movies_statement.executeQuery();
		while (movie_results.next()) {
			Movie movie = new Movie(movie_results.getString("tconst"), movie_results.getString("primaryTitle"), movie_results.getInt("startYear"), Set.of((String[]) movie_results.getArray("genres").getArray()));
			actors_stament.setString(1, movie_results.getString("tconst"));
			ResultSet actor_results = actors_stament.executeQuery();
			while (actor_results.next()) {
				movie.actorNames.add(actor_results.getString("primaryName"));
			}
			movies.add(movie);
		}
		return movies;
	}

	@Override
	public List<Actor> queryActors(
		@NotNull Connection connection,
		@NotNull String keywords
	) throws SQLException {
		logger.info(keywords);
		List<Actor> actors = new ArrayList<>();
		PreparedStatement actors_statement = connection.prepareStatement("SELECT nconst, primaryname FROM nbasics NATURAL JOIN tprincipals WHERE primaryname LIKE ? AND (category='actor' OR category='actress') GROUP BY nconst, primaryname ORDER BY COUNT(DISTINCT tconst) DESC, primaryname LIMIT 5");
		PreparedStatement movies_statement = connection.prepareStatement("SELECT \"primaryTitle\" FROM tmovies NATURAL JOIN tprincipals WHERE nconst = ? AND (category ='actor' OR category = 'actress') ORDER BY \"startYear\" DESC, \"primaryTitle\" NULLS LAST LIMIT 5");
		PreparedStatement costars_statement = connection.prepareStatement("WITH filme AS (SELECT DISTINCT tconst FROM tprincipals WHERE nconst = ?) SELECT primaryname, COUNT(DISTINCT tconst) AS anzahl FROM tprincipals NATURAL JOIN filme NATURAL JOIN nbasics WHERE nconst != ? AND (category = 'actor' OR category = 'actress') GROUP BY nconst, primaryname ORDER BY anzahl DESC, primaryname LIMIT 5");
		actors_statement.setString(1, "%" + keywords + "%");
		ResultSet actors_results = actors_statement.executeQuery();
		while (actors_results.next()) {
			movies_statement.setString(1, actors_results.getString("nconst"));
			costars_statement.setString(1, actors_results.getString("nconst"));
			costars_statement.setString(2, actors_results.getString("nconst"));
			ResultSet movies_results = movies_statement.executeQuery();
			ResultSet costars_results = costars_statement.executeQuery();
			Actor new_actor = new Actor(actors_results.getString("nconst"), actors_results.getString("primaryname"));
			while (movies_results.next()) {
				new_actor.playedIn.add(movies_results.getString("primaryTitle"));
			}
			while (costars_results.next()) {
				new_actor.costarNameToCount.put(costars_results.getString("primaryName"), costars_results.getInt("anzahl"));
			}
			actors.add(new_actor);
		}
		return actors;
	}
}
