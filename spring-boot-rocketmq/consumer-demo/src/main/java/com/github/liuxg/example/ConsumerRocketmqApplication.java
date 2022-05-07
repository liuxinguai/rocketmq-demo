package com.github.liuxg.example;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.annotation.Input;
import org.springframework.cloud.stream.binder.PollableMessageSource;
import org.springframework.context.annotation.Bean;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.messaging.SubscribableChannel;

/**
 * @author xinguai.liu
 */
@SpringBootApplication
@EnableBinding({ConsumerRocketmqApplication.MySink.class})
public class ConsumerRocketmqApplication {

    public static void main(String[] args) {
        SpringApplication.run(ConsumerRocketmqApplication.class, args);
    }


    @Bean
    public ConsumerCustomRunner customRunner() {
        return new ConsumerCustomRunner();
    }

    public interface MySink {

        @Input("input1")
        SubscribableChannel input1();

        @Input("input2")
        SubscribableChannel input2();

        @Input("input3")
        SubscribableChannel input3();

        @Input("input4")
        SubscribableChannel input4();

        @Input("input5")
        PollableMessageSource input5();

    }

    public static class ConsumerCustomRunner implements CommandLineRunner {

        @Autowired
        private MySink mySink;

        @Override
        public void run(String... args) throws InterruptedException {
            while (true) {
                mySink.input5().poll(m -> {
                    String payload = (String) m.getPayload();
//                    if (payload.contains("0")) {
//                        throw new IllegalArgumentException(
//                                "111111111111111111111111111111111111111111");
//                    }
                    System.out.println("pull msg: " + payload);
                }, new ParameterizedTypeReference<String>() {
                });
                Thread.sleep(5_00);
            }
        }

    }
}
