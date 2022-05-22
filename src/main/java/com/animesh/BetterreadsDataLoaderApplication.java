package com.animesh;

import com.animesh.author.Author;
import com.animesh.author.AuthorRepository;
import com.animesh.book.Book;
import com.animesh.book.BookRepository;
import com.animesh.connection.DataStaxAstraProperties;

import netscape.javascript.JSObject;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import org.springframework.boot.autoconfigure.cassandra.CqlSessionBuilderCustomizer;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@SpringBootApplication
@EnableConfigurationProperties(DataStaxAstraProperties.class)
public class BetterreadsDataLoaderApplication {

	@Autowired
	AuthorRepository authorRepository;

	@Autowired
	BookRepository bookRepository;

	@Value("${datadump.location.author}")  //Spring Expression Language
	private String authorDumpLocation;

	@Value("${datadump.location.works}")
	private String worksDumpLocation;

	public static void main(String[] args) {

		SpringApplication.run(BetterreadsDataLoaderApplication.class, args);
		//Tries to connect to local cassandra through a port --first time
	}


	private void initAuthors(){

		//TODO 1 : Read and parse the line
		Path path = Paths.get(authorDumpLocation);
		try (Stream<String> lines = Files.lines(path)) {
//              lines.limit(10).forEach(line->{
			lines.forEach(line->{
				  String jsonString = line.substring(line.indexOf("{"));

				  // TODO 2 :Construct Author Object
				  try {
					  JSONObject jsonObject = new JSONObject(jsonString);
					  Author author=new Author();
					  author.setName(jsonObject.optString("name"));
					  author.setPersonalName(jsonObject.optString("personal_name"));
					  author.setId(jsonObject.optString("key").replace("/authors/",""));

					  //TODO 3: Persist using Repository
					  authorRepository.save(author);
					  System.out.println("Inserted record for authorName: "+author.getName() + " ....");
				  } catch (JSONException e) {
					  e.printStackTrace();
				  }
			  });
		}catch (IOException e){
			e.printStackTrace();
		}


	}

	private void initWorks(){
		Path path = Paths.get(worksDumpLocation);

		// TODO : Creating the formatter : Text '2008-04-01T03:28:50.625462' could not be parsed
		DateTimeFormatter dateFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSSS");//'T' ignore This

		try (Stream<String> lines = Files.lines(path)) {
			lines.forEach(line->{
				String jsonString = line.substring(line.indexOf("{"));
				try {
					JSONObject jsonObject = new JSONObject(jsonString);
					// TODO 2 :Construct Book Object
					Book book = new Book();
				    book.setId(jsonObject.getString("key").replace("/works/",""));

					book.setName(jsonObject.optString("title"));

					JSONObject descriptionObj = jsonObject.optJSONObject("description");
                    if(descriptionObj!=null) {
						book.setName(descriptionObj.optString("value"));
					}

					JSONObject publishedObj = jsonObject.optJSONObject("created");
					if(publishedObj!=null) {
						String dateStr=publishedObj.optString("value");
						book.setPublishedDate(LocalDate.parse(dateStr,dateFormat));
					}

					JSONArray coverJSONArr = jsonObject.optJSONArray("covers");
					if(coverJSONArr!=null){
						List<String> coverIds = new ArrayList<>();
						for(int i=0;i<coverJSONArr.length();i++){
							coverIds.add(coverJSONArr.getString(i));
						}
						book.setCoversIds(coverIds);
					}

					JSONArray authorsJSONArr = jsonObject.optJSONArray("authors");
					if(authorsJSONArr!=null){
						List<String> authorIds = new ArrayList<>();
						for(int i=0;i<authorsJSONArr.length();i++){
							String authorId = authorsJSONArr.getJSONObject(i).
									getJSONObject("author").getString("key")
									.replace("/authors/", "");

							authorIds.add(authorId);

						}
						book.setAuthorIds(authorIds);

						//TODO : fetching the AuthorName from Cassandra and Mapping AuthorName and AuthorId
						List<String> authorNames = authorIds.stream().map(id -> authorRepository.findById(id))
								.map(optionalAuthor -> {
									if (!optionalAuthor.isPresent()) return "Unknown Author";
									return optionalAuthor.get().getName();
								}).collect(Collectors.toList());

						book.setAuthorNames(authorNames);
					}

					bookRepository.save(book);
					System.out.println("Insert Book for name :"+book.getName() + " ....");


				} catch (JSONException e) {
					e.printStackTrace(); // One Issue it breaks the prr
				}


			});
		}catch (IOException e){
			e.printStackTrace();
		}

	}



	@PostConstruct 	//The dependencies of some of the beans in the application context form a cycle:
	public void start(){

		initAuthors();
		initWorks();

//		Author author = new Author();
//		author.setId("id");
//		author.setName("Name");
//		author.setPersonalName("personalName");
//		authorRepository.save(author);


//		System.out.println("Application Started");
	}






	@Bean
	public CqlSessionBuilderCustomizer sessionBuilderCustomizer(DataStaxAstraProperties astraProperties) {
		Path bundle = astraProperties.getSecureConnectBundle().toPath();
		return builder -> builder.withCloudSecureConnectBundle(bundle);
	}
    //Giving the Spring Data Cassandra ability to connect to Astra Instance by secure-bundle which I have downloaded
}
