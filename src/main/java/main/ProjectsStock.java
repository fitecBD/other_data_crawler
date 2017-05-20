package main;

import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.Document;

public class ProjectsStock {
	private static Logger logger = LogManager.getLogger(ProjectsStock.class);
	List<Document> projects;

	public ProjectsStock(List<Document> projects) {
		super();
		this.projects = projects;
	}

	synchronized public Document getNextProject() {
		if (!this.projects.isEmpty()) {
			// int randomNum = ThreadLocalRandom.current().nextInt(0,
			// this.places.size());
			Document toReturn = this.projects.get(0);
			this.projects.remove(0);
			logger.info(this.projects.size() + " projects left");
			return toReturn;
		} else {
			return null;
		}
	}
}
