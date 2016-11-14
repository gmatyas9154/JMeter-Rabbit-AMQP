package com.zeroclue.jmeter.protocol.amqp;

import com.rabbitmq.client.*;
import org.apache.commons.lang3.StringUtils;
import org.apache.jmeter.config.Arguments;
import org.apache.jmeter.samplers.Entry;
import org.apache.jmeter.samplers.Interruptible;
import org.apache.jmeter.samplers.SampleResult;
import org.apache.jmeter.testelement.property.TestElementProperty;
import org.apache.jorphan.logging.LoggingManager;
import org.apache.log.Logger;

import java.io.IOException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by tudor.marc on 11/9/2016.
 */
public class AMQPRPCClient extends AMQPSampler implements Interruptible {
  private static final Logger log = LoggingManager.getLoggerForClass();
  private static final long serialVersionUID = -6065447856467238801L;

  private static final int DEFAULT_PREFETCH_COUNT = 0; // unlimited
  public static final String DEFAULT_PREFETCH_COUNT_STRING = Integer.toString(DEFAULT_PREFETCH_COUNT);

  public static boolean DEFAULT_USE_TX = false;
  private final static String USE_TX = "AMQPRPCClient.UseTx";

  public static boolean DEFAULT_PERSISTENT = false;
  private final static String PERSISTENT = "AMQPRPCClient.Persistent";

  //++ These are JMX names, and must not be changed
  private final static String MESSAGE = "AMQPRPCClient.Message";
  private final static String MESSAGE_ROUTING_KEY = "AMQPRPCClient.MessageRoutingKey";
  private final static String MESSAGE_TYPE = "AMQPRPCClient.MessageType";
  private final static String REPLY_TO_QUEUE = "AMQPRPCClient.ReplyToQueue";
  private final static String CONTENT_TYPE = "AMQPRPCClient.ContentType";
  private final static String CORRELATION_ID = "AMQPRPCClient.CorrelationId";
  private final static String MESSAGE_ID = "AMQPRPCClient.MessageId";
  private final static String HEADERS = "AMQPRPCClient.Headers";
  private static final String PREFETCH_COUNT = "AMQPRPCClient.PrefetchCount";
  private static final String RECEIVE_TIMEOUT = "AMQPRPCClient.ReceiveTimeout";

  private transient Channel channel;
  private transient QueueingConsumer consumer;
  private transient String consumerTag;

  @Override
  public SampleResult sample(Entry entry) {
    SampleResult result = new SampleResult();
    result.setSampleLabel(getName());
    result.setSuccessful(false);
    result.setResponseCode("500");

    try {
      initChannel();

      // only do this once per thread. Otherwise it slows down the consumption by appx 50%
      if (consumer == null) {
        log.info("Creating consumer");
        consumer = new QueueingConsumer(channel);
      }
      log.info("Starting basic consumer");
      String replyQueue = channel.queueDeclare().getQueue();
      log.info("Created reply to queue: " + replyQueue);
      setReplyToQueue(replyQueue);
      consumerTag = channel.basicConsume(getReplyToQueue(), true, consumer);
    } catch (Exception ex) {
      log.error("Failed to initialize channel : ", ex);
      result.setResponseMessage(ex.toString());
      return result;
    }

    String data = getMessage(); // Sampler data

    result.setSampleLabel(getTitle());
        /*
         * Perform the sampling
         */

    // aggregate samples.
    int loop = getIterationsAsInt();
    result.sampleStart(); // Start timing

    QueueingConsumer.Delivery delivery = null;
    try {
      AMQP.BasicProperties messageProperties = getProperties();
      byte[] messageBytes = getMessageBytes();

      for (int idx = 0; idx < loop; idx++) {
        log.info("sending message: " + getMessage() + ", with properties: " + messageProperties);
        channel.basicPublish(getExchange(), getMessageRoutingKey(), messageProperties, messageBytes);

        delivery = consumer.nextDelivery(getReceiveTimeoutAsInt());

        if (delivery == null) {
          result.setResponseMessage("timed out");

          return result;
        }

        /*
                 * Set up the sample result details
                 */
        String response = new String(delivery.getBody());
        result.setSamplerData(response);
        result.setResponseMessage(response);

        // commit the sample.
        if (getUseTx()) {
          channel.txCommit();
        }
      }

      result.setResponseData("OK", null);
      result.setDataType(SampleResult.TEXT);

      result.setResponseCodeOK();

      result.setSuccessful(true);

    } catch (ShutdownSignalException e) {
      consumer = null;
      consumerTag = null;
      log.warn("AMQP consumer failed to consume", e);
      result.setResponseCode("400");
      result.setResponseMessage(e.getMessage());
      interrupt();
    } catch (ConsumerCancelledException e) {
      consumer = null;
      consumerTag = null;
      log.warn("AMQP consumer failed to consume", e);
      result.setResponseCode("300");
      result.setResponseMessage(e.getMessage());
      interrupt();
    } catch (InterruptedException e) {
      consumer = null;
      consumerTag = null;
      log.info("interuppted while attempting to consume");
      result.setResponseCode("200");
      result.setResponseMessage(e.getMessage());
    } catch (IOException e) {
      consumer = null;
      consumerTag = null;
      log.warn("AMQP consumer failed to consume", e);
      result.setResponseCode("100");
      result.setResponseMessage(e.getMessage());
    } finally {
      result.sampleEnd(); // End timimg
      try {
        channel.basicCancel(consumerTag);
        consumerTag = null;
      } catch (IOException e) {
        log.error("Couldn't safely cancel the sample " + consumerTag, e);
      }
    }

    return result;
  }

  @Override
  protected Channel getChannel() {
    return channel;
  }

  @Override
  protected void setChannel(Channel channel) {
    this.channel = channel;
  }

  @Override
  public boolean interrupt() {
    cleanup();

    return true;
  }

  public String getMessageRoutingKey() {
    return getPropertyAsString(MESSAGE_ROUTING_KEY);
  }

  public void setMessageRoutingKey(String content) {
    setProperty(MESSAGE_ROUTING_KEY, content);
  }

  /**
   * @return the message for the sample
   */
  public String getMessage() {
    return getPropertyAsString(MESSAGE);
  }

  public void setMessage(String content) {
    setProperty(MESSAGE, content);
  }

  /**
   * @return the message type for the sample
   */
  public String getMessageType() {
    return getPropertyAsString(MESSAGE_TYPE);
  }

  public void setMessageType(String content) {
    setProperty(MESSAGE_TYPE, content);
  }

  /**
   * @return the reply-to queue for the sample
   */
  public String getReplyToQueue() {
    return getPropertyAsString(REPLY_TO_QUEUE);
  }

  public void setReplyToQueue(String content) {
    setProperty(REPLY_TO_QUEUE, content);
  }

  public String getContentType() {
    return getPropertyAsString(CONTENT_TYPE);
  }

  public void setContentType(String contentType) {
    setProperty(CONTENT_TYPE, contentType);
  }

  /**
   * @return the correlation identifier for the sample
   */
  public String getCorrelationId() {
    return getPropertyAsString(CORRELATION_ID);
  }

  public void setCorrelationId(String content) {
    setProperty(CORRELATION_ID, content);
  }

  /**
   * @return the message id for the sample
   */
  public String getMessageId() {
    return getPropertyAsString(MESSAGE_ID);
  }

  public void setMessageId(String content) {
    setProperty(MESSAGE_ID, content);
  }

  public Arguments getHeaders() {
    return (Arguments) getProperty(HEADERS).getObjectValue();
  }

  public void setHeaders(Arguments headers) {
    setProperty(new TestElementProperty(HEADERS, headers));
  }

  protected int getReceiveTimeoutAsInt() {
    if (getPropertyAsInt(RECEIVE_TIMEOUT) < 1) {
      return DEFAULT_TIMEOUT;
    }
    return getPropertyAsInt(RECEIVE_TIMEOUT);
  }

  public String getReceiveTimeout() {
    return getPropertyAsString(RECEIVE_TIMEOUT, DEFAULT_TIMEOUT_STRING);
  }


  public void setReceiveTimeout(String s) {
    setProperty(RECEIVE_TIMEOUT, s);
  }

  public String getPrefetchCount() {
    return getPropertyAsString(PREFETCH_COUNT, DEFAULT_PREFETCH_COUNT_STRING);
  }

  public void setPrefetchCount(String prefetchCount) {
    setProperty(PREFETCH_COUNT, prefetchCount);
  }

  public int getPrefetchCountAsInt() {
    return getPropertyAsInt(PREFETCH_COUNT);
  }

  public Boolean getUseTx() {
    return getPropertyAsBoolean(USE_TX, DEFAULT_USE_TX);
  }

  public void setUseTx(Boolean tx) {
    setProperty(USE_TX, tx);
  }

  public Boolean getPersistent() {
    return getPropertyAsBoolean(PERSISTENT, DEFAULT_PERSISTENT);
  }

  public void setPersistent(Boolean persistent) {
    setProperty(PERSISTENT, persistent);
  }

  protected AMQP.BasicProperties getProperties() {
    final AMQP.BasicProperties.Builder builder = new AMQP.BasicProperties.Builder();

    final int deliveryMode = getPersistent() ? 2 : 1;
    final String contentType = StringUtils.defaultIfEmpty(getContentType(), "text/plain");

    builder.contentType(contentType)
            .deliveryMode(deliveryMode)
            .priority(0)
            .correlationId(getCorrelationId())
            .replyTo(getReplyToQueue())
            .type(getMessageType())
            .headers(prepareHeaders())
            .build();
    if (getMessageId() != null && getMessageId().isEmpty()) {
      builder.messageId(getMessageId());
    }
    return builder.build();
  }

  protected boolean initChannel() throws IOException, NoSuchAlgorithmException, KeyManagementException {
    boolean ret = super.initChannel();
    if (getUseTx()) {
      channel.txSelect();
    }
    channel.basicQos(getPrefetchCountAsInt());
    return ret;
  }

  private byte[] getMessageBytes() {
    return getMessage().getBytes();
  }

  private Map<String, Object> prepareHeaders() {
    Map<String, Object> result = new HashMap<String, Object>();
    Map<String, String> source = getHeaders().getArgumentsAsMap();
    for (Map.Entry<String, String> item : source.entrySet()) {
      result.put(item.getKey(), item.getValue());
    }
    return result;
  }
}
