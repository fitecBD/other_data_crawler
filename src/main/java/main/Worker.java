package main;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.configuration2.builder.FileBasedConfigurationBuilder;
import org.apache.commons.configuration2.builder.fluent.Parameters;
import org.apache.commons.configuration2.convert.DefaultListDelimiterHandler;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.tika.langdetect.OptimaizeLangDetector;
import org.apache.tika.language.detect.LanguageDetector;
import org.bson.BsonArray;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCursor;

import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations;
import edu.stanford.nlp.util.CoreMap;

/**
 * Hello world!
 *
 */
public class Worker implements Runnable {

	private static Logger logger = LogManager.getLogger(Worker.class);

	private MongoCursor<org.bson.Document> cursor;

	private PropertiesConfiguration config;

	private String mongoUri;
	private String databaseName;
	private String collectionName;

	LanguageDetector detector = new OptimaizeLangDetector();

	// pipeline du module Stanford Core NLP
	StanfordCoreNLP stanfordSentiementPipeline;

	private File projetsKOFile = new File("projetsKO.txt");
	private File projetsOKFile = new File("projetsOK.txt");

	private static int cptProjects = 0;

	public Worker() throws ConfigurationException {
		super();
		FileBasedConfigurationBuilder<PropertiesConfiguration> builder = new FileBasedConfigurationBuilder<PropertiesConfiguration>(
				PropertiesConfiguration.class).configure(
						new Parameters().properties().setFileName("config.properties").setThrowExceptionOnMissing(true)
								.setListDelimiterHandler(new DefaultListDelimiterHandler(';'))
								.setIncludesAllowed(false));
		this.config = builder.getConfiguration();
		initFromProperties();
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
		int nbUpdates = 0;
		try {
			nbUpdates = Integer.parseInt(projectJsoupDocument.select("[data-content=updates] .count").text());
		} catch (Exception e) {
			e.printStackTrace();
			logger.error(e);
		}

		// update des updates en tant que telles
		// if (nbUpdates != 0 && nbUpdates !=
		// projectMongoDocument.getInteger("updates_count")) {
		if (nbUpdates != 0) {
			projectMongoDocument.put("updates_count", nbUpdates);
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
		// if (nbComments != 0 && nbComments !=
		// projectMongoDocument.getInteger("comments_count")) {
		if (nbComments != 0) {
			projectMongoDocument.put("comments_count", nbComments);

			String urlComments = projectMongoDocument.get("urls", org.bson.Document.class)
					.get("web", org.bson.Document.class).getString("project") + "/comments";

			org.bson.Document comments = new org.bson.Document();
			comments.put("data", new BsonArray());
			logger.info("scraping comments  : " + projectMongoDocument.getString("slug"));

			boolean olderCommentToScrape = false;
			int cptPage = 0;

			// le vecteur des sentiments agrégés pour toutes les phrases de tous
			// les commentaires
			// int[] allCommentsSentiments = { 0, 0, 0, 0, 0 };

			do {
				cptPage++;
				logger.debug("scraping comments page #" + cptPage);
				Document docComments;
				docComments = Jsoup.connect(urlComments).get();

				Elements commentsElements = docComments.select(".comment");
				for (Element commentElement : commentsElements) {
					String comment = commentElement.select("p").text().replaceAll("\\s+", " ").trim();
					if (comment.contains("This comment has been removed by Kickstarter.")) {
						continue;
					}
					org.bson.BsonDocument commentObject = new org.bson.BsonDocument();
					commentObject.put("data", new BsonString(comment));

					// on ajoute le vecteur de sentiments du commentaires
					// on détecte la langue du commentaire - on ne la prend en
					// compte que si la confiance est haute

					// LanguageResult languageResult = detector.detect(comment);
					// if
					// (languageResult.getConfidence().equals(LanguageConfidence.HIGH))
					// {
					// commentObject.put("lang",
					// new BsonDocument("tika_optimaize", new
					// BsonString(languageResult.getLanguage())));
					// if ("en".equals(languageResult.getLanguage())) {
					// populateSentimentForOneComment(comment, commentObject,
					// allCommentsSentiments);
					// }
					// }

					populateBadgeForOneComment(commentElement, commentObject);
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

			// populateSentimentsForAllComments(comments,
			// allCommentsSentiments);

			projectMongoDocument.put("comments", comments);
		} else {
			logger.info("pas de commentaire pour le projet : " + projectMongoDocument.getString("slug"));
		}
	}

	private void populateBadgeForOneComment(Element commentElement, org.bson.BsonDocument commentObject) {
		// on regarde si la personne ayant commenté a un badge
		if (!commentElement.select(".repeat-creator-badge").isEmpty()) {
			commentObject.put("creator-badge", new BsonBoolean(true));
		}
		if (!commentElement.select(".superbacker-badge").isEmpty()) {
			commentObject.put("superbacker-badge", new BsonBoolean(true));
		}
	}

	private void populateSentimentsForAllComments(org.bson.Document comments, int[] allCommentsSentiments) {
		// ajout des sentiments pour tous les vecteurs
		int[] noSentiments = { 0, 0, 0, 0, 0 };
		if (!Objects.deepEquals(allCommentsSentiments, noSentiments)) {
			List<BsonInt32> sentimentsBson = new ArrayList<>();
			Arrays.stream(allCommentsSentiments).forEach(item -> sentimentsBson.add(new BsonInt32(item)));
			comments.put("sentiment",
					new BsonDocument("stanford", new BsonDocument("sentence-vector", new BsonArray(sentimentsBson))));
		}
	}

	private void populateSentimentForOneComment(String comment, org.bson.BsonDocument commentObject,
			int[] allCommentsSentiments) {
		Annotation annotation = stanfordSentiementPipeline.process(comment);
		List<CoreMap> sentences = annotation.get(CoreAnnotations.SentencesAnnotation.class);

		int[] sentiments = { 0, 0, 0, 0, 0 };
		for (CoreMap sentence : sentences) {
			String sentiment = sentence.get(SentimentCoreAnnotations.SentimentClass.class);
			switch (sentiment) {
			case "Very negative":
				sentiments[0]++;
				allCommentsSentiments[0]++;
				break;
			case "Negative":
				sentiments[1]++;
				allCommentsSentiments[1]++;
				break;
			case "Neutral":
				sentiments[2]++;
				allCommentsSentiments[2]++;
				break;
			case "Positive":
				sentiments[3]++;
				allCommentsSentiments[3]++;
				break;
			case "Very positive":
				sentiments[4]++;
				allCommentsSentiments[4]++;
				break;
			default:
				throw new IllegalStateException(sentiment
						+ " : sentiment should be either \"Very negative\", \"Negative\", \"Neutral\", \"Positive\", \"Very positive\"");
			}
		}
		List<BsonInt32> sentimentsBson = new ArrayList<>();
		Arrays.stream(sentiments).forEach(item -> sentimentsBson.add(new BsonInt32(item)));
		commentObject.put("sentiment",
				new BsonDocument("stanford", new BsonDocument("sentence-vector", new BsonArray(sentimentsBson))));
	}

	private int updateCommentsCount(org.bson.Document projectMongoDocument, Document projectJsoupDocument) {
		String nbCommentsString = projectJsoupDocument.select("[data-content=comments] .count").text();
		int nbComments;
		Pattern pattern = Pattern.compile("(\\d+.?\\d+)|\\d+");
		Matcher matcher = pattern.matcher(nbCommentsString);
		if (matcher.find()) {
			nbComments = Integer.parseInt(matcher.group().replaceAll("\\D", ""));
		} else {
			logger.error(
					"nombre de commentaires introuvable pour le projet : " + projectMongoDocument.getString("slug"));
			nbComments = 0;
		}
		return nbComments;
	}

	private void updateDocuments() throws IOException {
		try (MongoClient mongoClient = new MongoClient(new MongoClientURI(mongoUri));) {
			// Construction de la requête
			BasicDBObject query = new BasicDBObject();
			// query.put("id", 1794394128);
			// query.put("id", 970251439);
			// query.put("id", 786189898);

			// on ajoute les ids à un hashset
			logger.info("getting projects ids from mongo database");
			try (MongoCursor<org.bson.Document> cursor = mongoClient.getDatabase(databaseName)
					.getCollection(collectionName).find(query).iterator()) {
				while (cursor.hasNext()) {
					org.bson.Document document = cursor.next();
					updateProject(mongoClient, document);
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private void updateProject(MongoClient mongoClient, org.bson.Document document) throws IOException {
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
		BasicDBObject updateFields = new BasicDBObject().append("description", document.getString("description"))
				.append("risks", document.getString("risks")).append("faq_count", document.getInteger("faq_count"))
				.append("updates_count", document.getInteger("updates_count"))
				.append("comments_count", document.getInteger("comments_count"));

		if (document.containsKey("updates")) {
			updateFields.append("updates", document.get("updates", org.bson.Document.class));
		}
		if (document.containsKey("comments")) {
			updateFields.append("comments", document.get("comments", org.bson.Document.class));
		}

		update.append("$set", updateFields);

		BasicDBObject searchQuery = new BasicDBObject().append("id", document.getInteger("id"));
		mongoClient.getDatabase(databaseName).getCollection(collectionName).findOneAndUpdate(searchQuery, update);
	}

	private void initFromProperties() {
		// mongo
		this.mongoUri = config.getString("mongo.uri");
		this.databaseName = config.getString("mongo.database");
		this.collectionName = config.getString("mongo.collection");

		// stanford core nlp
		String[] stanfordNlpAnnotators = config.getStringArray("stanford.corenlp.annotators");
		Properties stanfordNlpProps = new Properties();
		stanfordNlpProps.setProperty("annotators", String.join(",", stanfordNlpAnnotators));
		stanfordSentiementPipeline = new StanfordCoreNLP(stanfordNlpProps);

		// optimaize language detector
		try {
			detector.loadModels();
		} catch (IOException e) {
			logger.error(e);
			e.printStackTrace();
		}
	}

	public void setCursor(MongoCursor<org.bson.Document> cursor) {
		this.cursor = cursor;
	}

	private synchronized org.bson.Document getNextProject() {
		return (cursor.hasNext()) ? cursor.next() : null;
	}

	@Override
	public void run() {
		org.bson.Document projectBson;
		try (MongoClient mongoClient = new MongoClient(new MongoClientURI(mongoUri));) {
			while ((projectBson = getNextProject()) != null) {
				try {
					Worker.incrementCptProjects();
					logger.info(Worker.getCptProjects() + " projects");
					// throw new RuntimeException();
					int idProjet = projectBson.getInteger("id");
					if (!App.getInstance().getAlreadyCrawledprojectsIds().contains(idProjet)) {
						updateProject(mongoClient, projectBson);
						writeOKProject(idProjet);
					} else {
						logger.info("skipping project : " + idProjet + " - " + projectBson.getString("slug"));
					}
				} catch (Exception e) {
					e.printStackTrace();
					try {
						writeKOProject(projectBson.getInteger("id"));
					} catch (IOException e1) {
						// TODO Auto-generated catch block
						logger.error(e1);
						e1.printStackTrace();
					}
				}
			}
		}
	}

	private synchronized void writeOKProject(int id) throws IOException {
		FileUtils.writeStringToFile(projetsOKFile, id + "\n", StandardCharsets.UTF_8, true);
	}

	private synchronized void writeKOProject(int id) throws IOException {
		FileUtils.writeStringToFile(projetsKOFile, id + "\n", StandardCharsets.UTF_8, true);
	}

	public synchronized static int getCptProjects() {
		return cptProjects;
	}

	public static synchronized void incrementCptProjects() {
		Worker.cptProjects++;
	}
}
