package springbatchexample.springbatch.integration;

import java.io.File;
import java.time.Duration;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.launch.support.TaskExecutorJobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.integration.launch.JobLaunchingGateway;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.SyncTaskExecutor;
import org.springframework.integration.annotation.IntegrationComponentScan;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.config.EnableIntegration;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.Pollers;
import org.springframework.integration.file.DefaultFileNameGenerator;
import org.springframework.integration.file.FileReadingMessageSource;
import org.springframework.integration.file.FileWritingMessageHandler;
import org.springframework.integration.file.filters.SimplePatternFileListFilter;
import org.springframework.integration.file.support.FileExistsMode;
import org.springframework.stereotype.Component;

import lombok.RequiredArgsConstructor;

@Component
@EnableIntegration
@IntegrationComponentScan
@RequiredArgsConstructor
@Configuration
public class SalesInfoIntegrationConfig {

    @Value("${sales.info.directory}")
    private String salesDirectory;

    private final Job job;

    private final JobRepository jobRepository;
    
    //Read .csv files from a directory
    public FileReadingMessageSource fileReadingMessageSource() {
        var messageSource = new FileReadingMessageSource();
        messageSource.setDirectory(new File(salesDirectory));
        messageSource.setFilter(new SimplePatternFileListFilter("*.csv"));

    return messageSource;
    }

    //Create a channel to send the file
    public DirectChannel fileIn() {
        return new DirectChannel();
    }

    //Writing file
    public FileWritingMessageHandler fileRenameProcessingHandler() {
        FileWritingMessageHandler handler = new FileWritingMessageHandler(new File(salesDirectory));
        handler.setFileExistsMode(FileExistsMode.REPLACE);
        handler.setDeleteSourceFiles(Boolean.TRUE);
        handler.setFileNameGenerator(new DefaultFileNameGenerator());
        handler.setFileNameGenerator(fileNameGenerator());
        handler.setExpectReply(Boolean.FALSE);
        
        return handler;
    }

    public DefaultFileNameGenerator fileNameGenerator() {
        var fileNameGenerator = new DefaultFileNameGenerator();
        fileNameGenerator.setExpression("payload.name + '.processing'");

        return fileNameGenerator;
    }

    //Launch the job

    //Step 1.
    public FileMessageToJobRequest fileMessageToJobRequest() {
        FileMessageToJobRequest fileMessageToJobRequest = new FileMessageToJobRequest();
        fileMessageToJobRequest.setJob(job);
        fileMessageToJobRequest.setFilename(salesDirectory);

        return fileMessageToJobRequest;
    }

    //Step 2.
    public JobLaunchingGateway jobLaunchingGateway() {
        TaskExecutorJobLauncher jobLauncher = new TaskExecutorJobLauncher();
        jobLauncher.setJobRepository(jobRepository);
        jobLauncher.setTaskExecutor(new SyncTaskExecutor());
        JobLaunchingGateway jobLaunchingGateway = new JobLaunchingGateway(jobLauncher);

        return jobLaunchingGateway;
    }


    //Step 3.
    @Bean
    public IntegrationFlow integrationFlow(JobLaunchingGateway jobLaunchingGateway) {
        return IntegrationFlow.from(fileReadingMessageSource(),
                sourcePolling -> sourcePolling.poller(Pollers.fixedDelay(Duration.ofSeconds(5))
                .maxMessagesPerPoll(1)))
                .channel(fileIn())
                .handle(fileRenameProcessingHandler())
                .transform(fileMessageToJobRequest())
                .handle(jobLaunchingGateway())
                .log()
                .get();
    }
}
