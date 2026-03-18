package com.ververica.flinktraining.exercises.datastream_java.state;

import com.ververica.flinktraining.exercises.datastream_java.datatypes.TaxiFare;
import com.ververica.flinktraining.exercises.datastream_java.datatypes.TaxiRide;
import com.ververica.flinktraining.exercises.datastream_java.sources.TaxiFareSource;
import com.ververica.flinktraining.exercises.datastream_java.sources.TaxiRideSource;
import com.ververica.flinktraining.exercises.datastream_java.utils.ExerciseBase;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.util.Collector;

/**
 * Соединение потоков поездок и оплаты с использованием состояния ValueState.
 */
public class RidesAndFaresExercise extends ExerciseBase {
	public static void main(String[] args) throws Exception {

		ParameterTool cliParams = ParameterTool.fromArgs(args);
		final String ridesPath = cliParams.get("rides", pathToRideData);
		final String faresPath = cliParams.get("fares", pathToFareData);

		final int rideDelay = 60;
		final int simulationSpeed = 1800;

		Configuration storageConfig = new Configuration();
		storageConfig.setString("state.backend", "filesystem");
		storageConfig.setString("state.savepoints.dir", "file:///tmp/savepoints");
		storageConfig.setString("state.checkpoints.dir", "file:///tmp/checkpoints");

		StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(storageConfig);
		env.setParallelism(ExerciseBase.parallelism);

		env.enableCheckpointing(10000L);
		CheckpointConfig checkpointCfg = env.getCheckpointConfig();
		checkpointCfg.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

		DataStream<TaxiRide> rides = env
				.addSource(rideSourceOrTest(new TaxiRideSource(ridesPath, rideDelay, simulationSpeed)))
				.filter((TaxiRide r) -> r.isStart)
				.keyBy(r -> r.rideId);

		DataStream<TaxiFare> fares = env
				.addSource(fareSourceOrTest(new TaxiFareSource(faresPath, rideDelay, simulationSpeed)))
				.keyBy(f -> f.rideId);

		// Связывание потоков через RichCoFlatMap для обогащения данных
		DataStream<Tuple2<TaxiRide, TaxiFare>> combinedStream = rides
				.connect(fares)
				.flatMap(new JoinFunction())
				.uid("enrichment-operator");

		printOrTest(combinedStream);

		env.execute("Rides and Fares Joiner");
	}

	public static class JoinFunction extends RichCoFlatMapFunction<TaxiRide, TaxiFare, Tuple2<TaxiRide, TaxiFare>> {
		private ValueState<TaxiRide> storedRide;
		private ValueState<TaxiFare> storedFare;

		@Override
		public void open(Configuration parameters) {
			storedRide = getRuntimeContext().getState(new ValueStateDescriptor<>("ride-state", TaxiRide.class));
			storedFare = getRuntimeContext().getState(new ValueStateDescriptor<>("fare-state", TaxiFare.class));
		}

		@Override
		public void flatMap1(TaxiRide ride, Collector<Tuple2<TaxiRide, TaxiFare>> output) throws Exception {
			TaxiFare fare = storedFare.value();
			if (fare != null) {
				storedFare.clear();
				output.collect(new Tuple2<>(ride, fare));
			} else {
				storedRide.update(ride);
			}
		}

		@Override
		public void flatMap2(TaxiFare fare, Collector<Tuple2<TaxiRide, TaxiFare>> output) throws Exception {
			TaxiRide ride = storedRide.value();
			if (ride != null) {
				storedRide.clear();
				output.collect(new Tuple2<>(ride, fare));
			} else {
				storedFare.update(fare);
			}
		}
	}
}