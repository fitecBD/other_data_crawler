package main;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.configuration2.builder.FileBasedConfigurationBuilder;
import org.apache.commons.configuration2.builder.fluent.Parameters;
import org.apache.commons.configuration2.convert.DefaultListDelimiterHandler;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.tika.langdetect.OptimaizeLangDetector;
import org.apache.tika.language.detect.LanguageDetector;
import org.json.JSONException;

import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCursor;

import edu.stanford.nlp.pipeline.StanfordCoreNLP;

/**
 * Hello world!
 *
 */
public class App {
	private static Logger logger = LogManager.getLogger(App.class);

	private PropertiesConfiguration config;

	private String mongoUri;
	private String databaseName;
	private String collectionName;

	LanguageDetector detector = new OptimaizeLangDetector();

	// pipeline du module Stanford Core NLP
	StanfordCoreNLP stanfordSentiementPipeline;

	// private ProjectsStock projectsStock;
	private List<Thread> workers = new ArrayList<>();
	private int nbThreads = 5;

	public static final String ALREADY_CRAWLED_PROJECTS_FILE_PATH = "projetsOK.txt";
	private static Set<Integer> alreadyCrawledprojectsIds = new HashSet<>();

	private static App instance = null;

	MongoCursor<org.bson.Document> cursor;

	private void initAlreadyCrawledProjectsIds() throws IOException {
		BufferedReader reader = new BufferedReader(new FileReader(ALREADY_CRAWLED_PROJECTS_FILE_PATH));
		String line = null;
		do {
			line = reader.readLine();
			if (StringUtils.isNotBlank(line)) {
				int projectId = Integer.parseInt(line);
				alreadyCrawledprojectsIds.add(projectId);
				System.out.println("retrieving already crawled projects : " + projectId);
			}
		} while (line != null);
		reader.close();
	}

	public App() throws ConfigurationException, IOException {
		super();
		FileBasedConfigurationBuilder<PropertiesConfiguration> builder = new FileBasedConfigurationBuilder<PropertiesConfiguration>(
				PropertiesConfiguration.class).configure(
						new Parameters().properties().setFileName("config.properties").setThrowExceptionOnMissing(true)
								.setListDelimiterHandler(new DefaultListDelimiterHandler(';'))
								.setIncludesAllowed(false));
		this.config = builder.getConfiguration();
		initFromProperties();
		initAlreadyCrawledProjectsIds();
	}

	public static void main(String[] args) throws JSONException, IOException, ConfigurationException {
		App app = App.getInstance();
		app.run();
	}

	private void run() throws ConfigurationException {
		ArrayList<org.bson.Document> list = new ArrayList<>(264000);
		// ArrayList<org.bson.Document> list = new ArrayList<>();
		try (MongoClient mongoClient = new MongoClient(new MongoClientURI(mongoUri));) {
			BasicDBObject query = new BasicDBObject();
			BasicDBObject filter = new BasicDBObject();
			filter.append("_id", 0).append("id", 1).append("urls", 1).append("slug", 1);

			// on ajoute les ids à un hashset
			logger.info("querying projects from mongo database");
			cursor = mongoClient.getDatabase(databaseName).getCollection(collectionName).find(query).projection(filter)
					.iterator();

			// init des threads
			startThreads();

			// attente de la terminaison des threads
			joinThreads();
			logger.info("update terminée pour la collection " + collectionName + " de la BD " + databaseName);
		}
	}

	private void startThreads() throws ConfigurationException {
		for (int i = 0; i < nbThreads; i++) {
			Worker worker = new Worker();
			worker.setCursor(cursor);
			Thread thread = new Thread(worker, "worker #" + i);
			workers.add(thread);
			thread.start();
		}
	}

	private void joinThreads() {
		for (Thread thread : workers) {
			try {
				thread.join();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				logger.error(e);
				e.printStackTrace();
			}
		}
	}

	private void initFromProperties() {
		// mongo
		this.mongoUri = config.getString("mongo.uri");
		this.databaseName = config.getString("mongo.database");
		this.collectionName = config.getString("mongo.collection");
	}

	public static App getInstance() throws ConfigurationException, IOException {
		if (App.instance == null) {
			App.instance = new App();
		}
		return App.instance;
	}

	public static Set<Integer> getAlreadyCrawledprojectsIds() {
		return alreadyCrawledprojectsIds;
	}

}
