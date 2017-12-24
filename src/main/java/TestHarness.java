import com.google.api.gax.batching.FlowControlSettings;
import com.google.api.gax.core.InstantiatingExecutorProvider;
import com.google.api.gax.rpc.AlreadyExistsException;
import com.google.cloud.pubsub.v1.*;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.*;
import org.joda.time.DateTime;
import org.threeten.bp.Duration;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class TestHarness {

    private static final String PROJECT_NAME = System.getenv("PROJECT_NAME");
    private static final String TEST_TOPIC = "test-dup-message";
    private static final String SUBSCRIPTION_NAME = "dup-message-subscription";
    private static final int ACK_DEADLINE_IN_SECONDS = 10;
    private static final long NO_OF_MESSAGES = 5;

    private static String currentTime() {
        return DateTime.now().toString();
    }


    private static Topic ensureTopicExists(String topic) throws IOException {
        TopicAdminClient topicAdminClient = TopicAdminClient.create();
        TopicName topicName = TopicName.of(PROJECT_NAME, topic);
        try {
            return topicAdminClient.createTopic(topicName);
        } catch (AlreadyExistsException e) {
            return topicAdminClient.getTopic(topicName);
        }
    }

    private static Subscription ensureSubscriptionExists(Topic topic) throws IOException {
        SubscriptionAdminClient subscriptionAdminClient = SubscriptionAdminClient.create();
        SubscriptionName subscriptionName = SubscriptionName.of(PROJECT_NAME, SUBSCRIPTION_NAME);
        try {
            return subscriptionAdminClient.createSubscription(subscriptionName, topic.getNameAsTopicName(), PushConfig.newBuilder().build(), ACK_DEADLINE_IN_SECONDS);
        } catch (AlreadyExistsException e) {
            return subscriptionAdminClient.getSubscription(subscriptionName);
        }
    }

    private static HashMap<String, AtomicLong> publishMessages(Topic topic, List<String> messages) throws Exception {
        Publisher publisher = null;
        try {
            publisher = Publisher.newBuilder(topic.getNameAsTopicName()).build();
            HashMap<String, AtomicLong> messageFrequency = new HashMap<>();
            for (String message : messages) {
                ByteString data = ByteString.copyFromUtf8(message);
                PubsubMessage pubsubMessage = PubsubMessage.newBuilder().setData(data).build();
                String messageId = publisher.publish(pubsubMessage).get();
                System.out.println(String.format("Published: messageId %s, message: %s", messageId, message));
                messageFrequency.put(messageId, new AtomicLong(0));
            }
            return messageFrequency;
        } finally {
            if (publisher != null) {
                publisher.shutdown();
            }
        }
    }

    private static MessageReceiver getReceiver(HashMap<String, AtomicLong> publishResults, long sleepTime) {
        return new MessageReceiver() {
            public void receiveMessage(PubsubMessage message, AckReplyConsumer consumer) {
                System.out.println(currentTime() + " got message: " + message.getData().toStringUtf8());
                publishResults.get(message.getMessageId()).incrementAndGet();
                try {
                    Thread.sleep(sleepTime);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                consumer.ack();
            }
        };
    }

    public static void main(String[] args) throws Exception {
        Topic topic = ensureTopicExists(TEST_TOPIC);
        System.out.println("Topic exists");
        Subscription subscription = ensureSubscriptionExists(topic);
        System.out.println("Subscription exists");
        System.out.println("Subscription has a Ack deadline of: " + ACK_DEADLINE_IN_SECONDS);
        List<String> messages = Stream.generate(() -> UUID.randomUUID().toString()).limit(NO_OF_MESSAGES).collect(Collectors.toList());
        final HashMap<String, AtomicLong> publishResults = publishMessages(topic, messages);
        System.out.println("Starting: " + currentTime());

        Subscriber subscriber = null;
        MessageReceiver receiver = getReceiver(publishResults, TimeUnit.MINUTES.toMillis(5));
        try {
            subscriber = Subscriber.newBuilder(subscription.getNameAsSubscriptionName(), receiver)
                    .setMaxAckExtensionPeriod(Duration.ofHours(8))
                    .setExecutorProvider(InstantiatingExecutorProvider.newBuilder().setExecutorThreadCount(5).build())
                    .setParallelPullCount(8)
                    .setFlowControlSettings(FlowControlSettings.newBuilder().setMaxOutstandingElementCount(5l).build())
                    .build();
            subscriber.addListener(
                    new Subscriber.Listener() {
                        @Override
                        public void failed(Subscriber.State from, Throwable failure) {
                            // Handle failure. This is called when the Subscriber encountered a fatal error and is shutting down.
                            System.err.println(currentTime() + " " + failure);
                        }
                    },
                    MoreExecutors.directExecutor());
            subscriber.startAsync().awaitRunning();
            Thread.sleep(TimeUnit.MINUTES.toMillis(30));
            System.out.println("finished: " + currentTime());
            System.out.println(publishResults);
        } finally {
            if (subscriber != null) {
                subscriber.stopAsync();
            }
        }
        System.out.println(publishResults);
    }


}
