package com.orwellg.yggdrasil.dsl.card.transactions.topology;

import com.orwellg.umbrella.commons.storm.topology.TopologyFactory;
import com.orwellg.umbrella.commons.storm.topology.component.bolt.EventErrorBolt;
import com.orwellg.umbrella.commons.storm.topology.component.spout.KafkaSpout;
import com.orwellg.umbrella.commons.storm.topology.generic.bolt.GBolt;
import com.orwellg.umbrella.commons.storm.topology.generic.bolt.GRichBolt;
import com.orwellg.umbrella.commons.storm.topology.generic.grouping.ShuffleGrouping;
import com.orwellg.umbrella.commons.storm.topology.generic.spout.GSpout;
import com.orwellg.umbrella.commons.storm.wrapper.kafka.KafkaBoltWrapper;
import com.orwellg.umbrella.commons.storm.wrapper.kafka.KafkaSpoutWrapper;
import com.orwellg.yggdrasil.dsl.card.transactions.topology.bolts.event.KafkaEventProcessBolt;
import com.orwellg.yggdrasil.dsl.card.transactions.topology.bolts.processors.AccountingBolt;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.StormTopology;

import java.util.Arrays;

public class CardPresentmentAccountingTopology {

    //1. send event to debit/credit the client
    //2. get wirecard account
    //3. send event do debit/credit wirecard account

    private final static Logger LOG = LogManager.getLogger(CardPresentmentAccountingTopology.class);

    public static void main(String[] args) throws Exception {

        LOG.debug("Creating Card Presentments processing topology");

        Integer hints = 1;

        //read from com.orwellg.gps.presentment.response
        GSpout kafkaEventReader = new GSpout("kafka-event-reader", new KafkaSpoutWrapper("subscriber-card-presentment-accounting.yaml", String.class, String.class).getKafkaSpout(), hints);

        // Parse the events and we send it to the rest of the topology
        GBolt<?> kafkaEventProcess = new GRichBolt("kafka-event-success-process", new KafkaEventProcessBolt(), hints);
        kafkaEventProcess.addGrouping(new ShuffleGrouping("kafka-event-reader", KafkaSpout.EVENT_SUCCESS_STREAM));

        // GBolt for work with the errors
        GBolt<?> kafkaEventError = new GRichBolt("kafka-event-error-process", new EventErrorBolt(), hints);
        kafkaEventError.addGrouping(new ShuffleGrouping("kafka-event-reader", KafkaSpout.EVENT_ERROR_STREAM));

        // GBolt for send errors of
        GBolt<?> kafkaErrorProducer = new GRichBolt("kafka-error-producer", new KafkaBoltWrapper("publisher-gps-dsl-error.yaml", String.class, String.class).getKafkaBolt(), hints);
        kafkaErrorProducer.addGrouping(new ShuffleGrouping("kafka-event-error-process"));

        //todo: get wirecard account?

        //create events to debit/credit client
        GBolt<?> accountingCommandsProducer = new GRichBolt("accounting-commands-producer", new AccountingBolt(), hints);
        accountingCommandsProducer.addGrouping(new ShuffleGrouping("kafka-event-success-process"));


        StormTopology topology = TopologyFactory.generateTopology(
                kafkaEventReader,
                Arrays.asList(kafkaEventProcess, kafkaEventError, kafkaErrorProducer, accountingCommandsProducer));
        LOG.debug("Topology created");

        // Create the basic config and upload the topology
        Config conf = new Config();
        conf.setDebug(false);
        conf.setMaxTaskParallelism(30);

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("card-presentment-accounting", conf, topology);

        Thread.sleep(3000000);
        cluster.shutdown();



    }



}
