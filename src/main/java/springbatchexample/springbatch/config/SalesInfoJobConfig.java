package springbatchexample.springbatch.config;

import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.integration.async.AsyncItemProcessor;
import org.springframework.batch.integration.async.AsyncItemWriter;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.data.RepositoryItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.LineMapper;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.task.TaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.transaction.PlatformTransactionManager;

import lombok.RequiredArgsConstructor;
import springbatchexample.springbatch.domain.SalesInfo;
import springbatchexample.springbatch.dto.SalesInfoDTO;
import springbatchexample.springbatch.processor.SalesInfoItemProcessor;
import springbatchexample.springbatch.repository.SalesInfoRepository;

@Configuration
@RequiredArgsConstructor
public class SalesInfoJobConfig {
    
    private final JobRepository jobRepository;

    private final PlatformTransactionManager platformTransactionManager;

    private final SalesInfoRepository salesInfoRepository;


    @Bean
    @StepScope
    public FlatFileItemReader<SalesInfoDTO> salesInfoFileReader(@Value("#{jobParameters['input.file.name']}") String resource) {

        FlatFileItemReader<SalesInfoDTO> itemReader = new FlatFileItemReader<>();
            itemReader.setResource(new FileSystemResource(resource));
            itemReader.setName("salesInfoFileReader");
            itemReader.setLinesToSkip(1);
            itemReader.setLineMapper(lineMapper());
            
        return itemReader;
    }

    private LineMapper<SalesInfoDTO> lineMapper() {
        DefaultLineMapper<SalesInfoDTO> lineMapper = new DefaultLineMapper<>();
        DelimitedLineTokenizer lineTokenizer = new DelimitedLineTokenizer();
        lineTokenizer.setDelimiter(",");
        lineTokenizer.setStrict(false);
        lineTokenizer.setNames("Product", "Seller", "Seller ID", "Price", "City", "Category");

        BeanWrapperFieldSetMapper<SalesInfoDTO> fieldSetMapper = new BeanWrapperFieldSetMapper<>();
        fieldSetMapper.setTargetType(SalesInfoDTO.class);
        lineMapper.setLineTokenizer(lineTokenizer);
        lineMapper.setFieldSetMapper(fieldSetMapper);

        return lineMapper;
    }

    @Bean
    public SalesInfoItemProcessor salesInfoItemProcessor() {
        return new SalesInfoItemProcessor();
    }

    @Bean
    public RepositoryItemWriter<SalesInfo> salesInfoIteamWriter() {
        RepositoryItemWriter<SalesInfo> writer = new RepositoryItemWriter<>();
        writer.setRepository(salesInfoRepository);
        writer.setMethodName("save");
        
        return writer;
    }

    @Bean
    public Step fromFileIntoDatabase(ItemReader<SalesInfoDTO> salesInfoDTOItemReader) {
        return new StepBuilder("fromFileIntoDatabase", jobRepository)
        //Here we return a Future<SalesInfo> after we process an item
            .<SalesInfoDTO, Future<SalesInfo>>chunk(100, platformTransactionManager)
            .reader(salesInfoDTOItemReader)
            .processor(asyncItemProcessor())
            .writer(asyncItemWriter())
            .taskExecutor(taskExecutor())
            .build();

    }

    @Bean
    public Job importSalesInfo(Step fromFileIntoDatabase) {
        return new JobBuilder("importSalesInfo", jobRepository)
            .incrementer(new RunIdIncrementer())
            .start(fromFileIntoDatabase)
            .build();
    }

    //Use of TaskExecutor to scale the application --> multi threads Step
    //It allows us to execute the chunk in a separate thread
    public TaskExecutor taskExecutor() {
        var executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(5);
        executor.setMaxPoolSize(5);
        executor.setQueueCapacity(10);
        executor.setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy());
        executor.setThreadNamePrefix("Thread N-> :");

    return executor;
    }

    @Bean
    public AsyncItemProcessor<SalesInfoDTO, SalesInfo> asyncItemProcessor() {
        var asyncItemProcessor = new AsyncItemProcessor<SalesInfoDTO, SalesInfo>();
        asyncItemProcessor.setDelegate(salesInfoItemProcessor());
        asyncItemProcessor.setTaskExecutor(taskExecutor());
        return asyncItemProcessor;
    }

    @Bean
    public AsyncItemWriter<SalesInfo> asyncItemWriter() {
        var asyncItemWriter = new AsyncItemWriter<SalesInfo>();
        asyncItemWriter.setDelegate(salesInfoIteamWriter());
        return asyncItemWriter;
    }
}
