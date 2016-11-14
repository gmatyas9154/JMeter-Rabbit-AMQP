package com.zeroclue.jmeter.protocol.amqp.gui;

import com.zeroclue.jmeter.protocol.amqp.AMQPRPCClient;
import org.apache.jmeter.config.Arguments;
import org.apache.jmeter.config.gui.ArgumentsPanel;
import org.apache.jmeter.testelement.TestElement;
import org.apache.jorphan.gui.JLabeledTextArea;
import org.apache.jorphan.gui.JLabeledTextField;

import javax.swing.*;
import java.awt.*;

/**
 * Created by tudor.marc on 11/9/2016.
 */
public class AMQPRPCClientGUI extends AMQPSamplerGui {

  private static final long serialVersionUID = 1L;
  private JPanel mainPanel;

  /*
  private static final String[] CONFIG_CHOICES = {"File", "Static"};
  private final JLabeledRadio configChoice = new JLabeledRadio("Message Source", CONFIG_CHOICES);
  private final FilePanel messageFile = new FilePanel("Filename", ALL_FILES);
  */
  private JLabeledTextArea message = new JLabeledTextArea("Message Content");
  private JLabeledTextField messageRoutingKey = new JLabeledTextField("Routing Key");
  private JLabeledTextField messageType = new JLabeledTextField("Message Type");
  private JLabeledTextField correlationId = new JLabeledTextField("Correlation Id");
  private JLabeledTextField contentType = new JLabeledTextField("ContentType");
  private JLabeledTextField messageId = new JLabeledTextField("Message Id");
  protected JLabeledTextField receiveTimeout = new JLabeledTextField("Receive Timeout");
  protected JLabeledTextField prefetchCount = new JLabeledTextField("Prefetch Count");

  private JCheckBox persistent = new JCheckBox("Persistent?", AMQPRPCClient.DEFAULT_PERSISTENT);
  private JCheckBox useTx = new JCheckBox("Use Transactions?", AMQPRPCClient.DEFAULT_USE_TX);

  private ArgumentsPanel headers = new ArgumentsPanel("Headers");

  public AMQPRPCClientGUI(){
    init();
  }

  @Override
  protected void setMainPanel(JPanel panel) {
    mainPanel = panel;
  }

  @Override
  public String getLabelResource() {
    return this.getClass().getSimpleName();
  }

  @Override
  public String getStaticLabel() {
    return "AMQP RPC Client";
  }

  @Override
  public TestElement createTestElement() {
    AMQPRPCClient sampler = new AMQPRPCClient();
    modifyTestElement(sampler);
    return sampler;
  }

  @Override
  public void modifyTestElement(TestElement te) {
    AMQPRPCClient sampler = (AMQPRPCClient) te;
    sampler.clear();
    configureTestElement(sampler);

    super.modifyTestElement(sampler);

    sampler.setPersistent(persistent.isSelected());
    sampler.setUseTx(useTx.isSelected());

    sampler.setMessageRoutingKey(messageRoutingKey.getText());
    sampler.setMessage(message.getText());
    sampler.setMessageType(messageType.getText());
    sampler.setCorrelationId(correlationId.getText());
    sampler.setContentType(contentType.getText());
    sampler.setMessageId(messageId.getText());
    sampler.setHeaders((Arguments) headers.createTestElement());
    sampler.setPrefetchCount(prefetchCount.getText());
    sampler.setReceiveTimeout(receiveTimeout.getText());
  }

  @Override
  public void configure(TestElement element) {
    super.configure(element);
    if (!(element instanceof AMQPRPCClient)) return;
    AMQPRPCClient sampler = (AMQPRPCClient) element;

    persistent.setSelected(sampler.getPersistent());
    useTx.setSelected(sampler.getUseTx());

    messageRoutingKey.setText(sampler.getMessageRoutingKey());
    messageType.setText(sampler.getMessageType());
    contentType.setText(sampler.getContentType());
    correlationId.setText(sampler.getCorrelationId());
    messageId.setText(sampler.getMessageId());
    message.setText(sampler.getMessage());
    prefetchCount.setText(sampler.getPrefetchCount());
    receiveTimeout.setText(sampler.getReceiveTimeout());
    configureHeaders(sampler);
  }

  @Override
  protected final void init() {
    super.init();
    persistent.setPreferredSize(new Dimension(100, 25));
    useTx.setPreferredSize(new Dimension(100, 25));
    messageRoutingKey.setPreferredSize(new Dimension(100, 25));
    messageType.setPreferredSize(new Dimension(100, 25));
    correlationId.setPreferredSize(new Dimension(100, 25));
    contentType.setPreferredSize(new Dimension(100, 25));
    messageId.setPreferredSize(new Dimension(100, 25));
    message.setPreferredSize(new Dimension(400, 150));
    prefetchCount.setPreferredSize(new Dimension(100,25));

    mainPanel.add(persistent);
    mainPanel.add(useTx);
    mainPanel.add(messageRoutingKey);
    mainPanel.add(messageType);
    mainPanel.add(correlationId);
    mainPanel.add(contentType);
    mainPanel.add(messageId);
    mainPanel.add(headers);
    mainPanel.add(prefetchCount);
    mainPanel.add(receiveTimeout);
    mainPanel.add(message);
  }

  @Override
  public void clearGui() {
    super.clearGui();
    persistent.setSelected(AMQPRPCClient.DEFAULT_PERSISTENT);
    useTx.setSelected(AMQPRPCClient.DEFAULT_USE_TX);
    messageRoutingKey.setText("");
    messageType.setText("");
    correlationId.setText("");
    contentType.setText("");
    messageId.setText("");
    headers.clearGui();
    message.setText("");
    prefetchCount.setText(AMQPRPCClient.DEFAULT_PREFETCH_COUNT_STRING);
    receiveTimeout.setText("");
  }

  private void configureHeaders(AMQPRPCClient sampler) {
    Arguments sampleHeaders = sampler.getHeaders();
    if (sampleHeaders != null) {
      headers.configure(sampleHeaders);
    } else {
      headers.clearGui();
    }
  }
}
