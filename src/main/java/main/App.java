package main;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.configuration2.builder.FileBasedConfigurationBuilder;
import org.apache.commons.configuration2.builder.fluent.Parameters;
import org.apache.commons.configuration2.convert.DefaultListDelimiterHandler;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.BsonArray;
import org.bson.BsonBoolean;
import org.bson.BsonString;
import org.json.JSONException;
import org.json.JSONObject;
import org.jsoup.Jsoup;
import org.jsoup.nodes.DataNode;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;

/**
 * Hello world!
 *
 */
public class App {
	private static Logger logger = LogManager.getLogger(App.class);

	private PropertiesConfiguration config;

	private String mongoUri = "mongodb://localhost:27017";
	private MongoClient mongoClient;
	private MongoDatabase mongoDatabase;
	@SuppressWarnings("rawtypes")
	private MongoCollection outputCollection;
	private String databaseName = "crowdfunding";
	private String collectionName = "kickstarter";
	private String username = "Fitec";
	private String password = "Fitecmongo";

	public App() throws ConfigurationException {
		super();
		FileBasedConfigurationBuilder<PropertiesConfiguration> builder = new FileBasedConfigurationBuilder<PropertiesConfiguration>(
				PropertiesConfiguration.class).configure(
						new Parameters().properties().setFileName("config.properties").setThrowExceptionOnMissing(true)
								.setListDelimiterHandler(new DefaultListDelimiterHandler(';'))
								.setIncludesAllowed(false));
		this.config = builder.getConfiguration();
		initFromProperties();
	}

	public static void main(String[] args) throws JSONException, IOException, ConfigurationException {
		App app = new App();
		app.updateDocuments();
		// app.updateTestProject();

		// testBadge(app);
	}

	private static void testBadge(App app) throws IOException {
		String url = "https://www.kickstarter.com/projects/dackley-mcphail/you-deserve-a-cookie";
		JSONObject projectMongoDocument = app.buildJSONObject(url);
		app.getComments(org.bson.Document.parse(projectMongoDocument.toString()), Jsoup.connect(url).get());
	}

	private static void updateTestProject() throws IOException {
		JSONObject jsonObject = new JSONObject(
				FileUtils.readFileToString(new File("project.json"), StandardCharsets.UTF_8));
		try (MongoClient mongoClient = new MongoClient(new MongoClientURI("mongodb://localhost:27017"));) {

			MongoDatabase database = mongoClient.getDatabase("crowdfunding");
			MongoCollection collection = database.getCollection("test");
			collection.insertOne(org.bson.Document.parse(jsonObject.toString()));
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private void getIdsProjects() {
	}

	private void getDescriptionRisksAndFAQ(org.bson.Document projectDocument) throws IOException {
		// update de la description
		String toReturn = null;
		Document doc;
		String url = projectDocument.get("urls", org.bson.Document.class).get("web", org.bson.Document.class)
				.getString("project") + "/description";
		logger.info("scraping descprition : " + projectDocument.getString("slug"));
		doc = Jsoup.connect(url).get();
		Elements scriptTags = doc.getElementsByClass("full-description");
		if (scriptTags != null && !scriptTags.isEmpty()) {
			toReturn = scriptTags.text().replaceAll("\\s+", " ").trim();
		}
		projectDocument.put("description", toReturn);

		// update des risques
		Elements risksElems = doc.select(".js-risks");
		if (risksElems != null && !risksElems.isEmpty()) {
			String risks = risksElems.text().replaceAll("\\s+", " ").trim();
			projectDocument.put("risks", risks);
		}

		// update des FAQ -- on ne récupère que leur nombre
		Elements faqElements = doc.select("[data-content=faqs] .count");
		int faqCounts = (!faqElements.isEmpty()) ? Integer.parseInt(faqElements.text()) : 0;
		projectDocument.put("faq_count", faqCounts);
	}

	private void getUpdates(org.bson.Document projectMongoDocument, Document projectJsoupDocument) throws IOException {
		// update du nombre d'updates
		int nbUpdates = Integer.parseInt(projectJsoupDocument.select("[data-content=updates] .count").text());
		projectMongoDocument.put("updates_count", nbUpdates);

		// update des updates en tant que telles
		if (nbUpdates != 0) {
			getUpdatesInner(projectMongoDocument);
		} else {
			logger.info("pas d'updates pour le projet : " + projectMongoDocument.getString("slug"));
		}
	}

	private void getUpdatesInner(org.bson.Document projectMongoDocument) throws IOException {
		int nbPage = 1;
		String urlUpdates = projectMongoDocument.get("urls", org.bson.Document.class)
				.get("web", org.bson.Document.class).getString("updates");
		org.bson.Document updates = new org.bson.Document();
		updates.put("data", new org.bson.BsonArray());
		logger.info("scraping updates  : " + projectMongoDocument.getString("slug"));

		Elements updateElements;
		do {
			String url = urlUpdates + "?page=" + nbPage;
			Document doc;
			doc = Jsoup.connect(url).get();
			// lecture updates une par une
			updateElements = doc.select(".post");
			for (Element updateElement : updateElements) {
				// logger.info("scraping update #" + cptUpdate++ + " of project
				// : " + projectJson.getString("slug"));
				String title = updateElement.select(".title").text().replaceAll("\\s+", " ").trim();
				String content = updateElement.select(".body").text().replaceAll("\\s+", " ").trim();

				org.bson.BsonDocument update = new org.bson.BsonDocument();
				update.append("data", new BsonString(String.join(" ", title, content)));
				updates.get("data", org.bson.BsonArray.class).add(update);
				// String metadata =
				// element.select("grid-post__metadata").text().replaceAll("\\s+",
				// " ").trim();
			}
			nbPage++;
		} while (updateElements != null && !updateElements.isEmpty());

		projectMongoDocument.put("updates", updates);
	}

	private void getComments(org.bson.Document projectMongoDocument, Document projectJsoupDocument) throws IOException {
		// mise à jour du nombre de documents
		int nbComments = updateCommentsCount(projectMongoDocument, projectJsoupDocument);

		// mise à jour des commentaires en tant que tels
		if (nbComments != 0) {
			org.bson.Document comments = new org.bson.Document();
			Elements commentsElements;

			String urlComments = projectMongoDocument.get("urls", org.bson.Document.class)
					.get("web", org.bson.Document.class).getString("project") + "/comments";

			comments.put("data", new BsonArray());
			logger.info("scraping comments  : " + projectMongoDocument.getString("slug"));

			boolean olderCommentToScrape = false;
			int cptDocuments = 0;
			int cptPage = 0;

			do {
				cptPage++;
				logger.debug("scraping comments page #" + cptPage);
				Document docComments;
				docComments = Jsoup.connect(urlComments).get();

				commentsElements = docComments.select(".comment");
				for (Element commentElement : commentsElements) {
					cptDocuments++;
					String comment = commentElement.select("p").text().replaceAll("\\s+", " ").trim();
					if (comment.contains("This comment has been removed by Kickstarter.")) {
						continue;
					}
					org.bson.BsonDocument commentObject = new org.bson.BsonDocument();
					commentObject.put("data", new BsonString(comment));

					// on regarde si la personne ayant commenté a un badge
					if (!commentElement.select(".repeat-creator-badge").isEmpty()) {
						commentObject.put("is_a_creator", new BsonBoolean(true));
					} else if (!commentElement.select(".superbacker-badge").isEmpty()) {
						commentObject.put("is_a_superbacker", new BsonBoolean(true));
					}
					comments.get("data", BsonArray.class).add(commentObject);
				}

				// récupération du lien "voir les commentaires plus anciens"
				Elements olderCommentsElements = docComments.select("a.older_comments");
				if (olderCommentsElements.size() > 0) {
					urlComments = "https://www.kickstarter.com"
							+ docComments.select("a.older_comments").get(0).attr("href");
					olderCommentToScrape = true;
				} else {
					olderCommentToScrape = false;
				}
			} while (olderCommentToScrape);

			projectMongoDocument.put("comments", comments);
		} else {
			logger.info("pas de commentaire pour le projet : " + projectMongoDocument.getString("slug"));
		}
	}

	private int updateCommentsCount(org.bson.Document projectMongoDocument, Document projectJsoupDocument) {
		String nbCommentsString = projectJsoupDocument.select("[data-content=comments] .count").text();
		int nbComments;
		Pattern pattern = Pattern.compile("(\\d+.?\\d+)|\\d+");
		Matcher matcher = pattern.matcher(nbCommentsString);
		if (matcher.find()) {
			nbComments = Integer.parseInt(matcher.group().replaceAll("\\D", ""));
		} else {
			throw new RuntimeException(
					"nombre de commentaires introuvable pour le projet : " + projectMongoDocument.getString("slug"));
		}
		projectMongoDocument.put("comments_count", nbComments);
		return nbComments;
	}

	private void updateDocuments() throws IOException {
		try (MongoClient mongoClient = new MongoClient(new MongoClientURI(mongoUri));) {
			// Construction de la requête
			BasicDBObject query = new BasicDBObject();
			// query.put("id", 2100439267);
			// query.put("id", 786189898);
			BasicDBObject fields = new BasicDBObject();
			// fields.put("id", 1);
			// fields.put("_id", 0);

			// on ajoute les ids à un hashset
			logger.info("getting projects ids from mongo database");
			try (MongoCursor<org.bson.Document> cursor = mongoClient.getDatabase(databaseName)
					.getCollection(collectionName).find(query).iterator()) {
				while (cursor.hasNext()) {
					org.bson.Document document = cursor.next();
					logger.info("updating project : " + document.getString("slug"));
					String urlProjet = document.get("urls", org.bson.Document.class).get("web", org.bson.Document.class)
							.getString("project");
					Document projectDocument;
					projectDocument = Jsoup.connect(urlProjet).get();
					getDescriptionRisksAndFAQ(document);
					getUpdates(document, projectDocument);
					getComments(document, projectDocument);

					// update en base
					BasicDBObject update = new BasicDBObject();
					BasicDBObject updateFields = new BasicDBObject()
							.append("description", document.getString("description"))
							.append("risks", document.getString("risks"))
							.append("faq_count", document.getInteger("faq_count"))
							.append("updates_count", document.getInteger("updates_count"))
							.append("updates", document.get("updates", org.bson.Document.class))
							.append("comments_count", document.getInteger("comments_count"))
							.append("comments", document.get("comments", org.bson.Document.class));
					update.append("$set", updateFields);

					BasicDBObject searchQuery = new BasicDBObject().append("id", document.getInteger("id"));
					// UpdateResult updateResult =
					mongoClient.getDatabase(databaseName).getCollection(collectionName).findOneAndUpdate(searchQuery,
							update);
					// System.out.println(updateResult.getModifiedCount());
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private JSONObject buildJSONObject(String url) throws IOException {
		Document doc;
		JSONObject jsonObject = null;
		logger.info("scraping project : " + url);
		doc = Jsoup.connect(url).get();
		Elements scriptTags = doc.getElementsByTag("script");
		for (Element tag : scriptTags) {
			for (DataNode node : tag.dataNodes()) {
				BufferedReader reader = new BufferedReader(new StringReader(node.getWholeData()));
				String line = null;
				do {
					line = reader.readLine();
					if (line != null && line.startsWith("  window.current_project")) {
						String jsonEncoded = line.substring(28, line.length() - 2);
						String jsonDecoded = StringEscapeUtils.unescapeHtml4(jsonEncoded).replaceAll("\\\\\\\\",
								"\\\\");
						jsonObject = new JSONObject(jsonDecoded);
					}
				} while (line != null);
				reader.close();
			}
		}

		return jsonObject;
	}

	private void initFromProperties() {
		this.mongoUri = config.getString("mongo.uri");
		this.databaseName = config.getString("mongo.database");
		this.collectionName = config.getString("mongo.collection");
	}
}
