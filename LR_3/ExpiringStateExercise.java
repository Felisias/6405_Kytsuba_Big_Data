package com.ververica.flinktraining.exercises.datastream_java.process;

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
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * Обогащение данных с контролем времени жизни состояния.
 * Если пара (поездка + оплата) не найдена вовремя, данные отправляются в Side Output.
 */
public class ExpiringStateExercise extends ExerciseBase {
	static final OutputTag<TaxiRide> lonelyRides = new OutputTag<TaxiRide>("unmatchedRides") {};
	static final OutputTag<TaxiFare> lonelyFares = new OutputTag<TaxiFare>("unmatchedFares") {};

	public static void main(String[] args) throws Exception {

		ParameterTool configParams = ParameterTool.fromArgs(args);
		final String rideSourceFile = configParams.get("rides", ExerciseBase.pathToRideData);
		final String fareSourceFile = configParams.get("fares", ExerciseBase.pathToFareData);

		final int delayThreshold = 60;
		final int throughputFactor = 600;

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		env.setParallelism(ExerciseBase.parallelism);

		DataStream<TaxiRide> rideData = env
				.addSource(rideSourceOrTest(new TaxiRideSource(rideSourceFile, delayThreshold, throughputFactor)))
				.filter((TaxiRide r) -> (r.isStart && (r.rideId % 1000 != 0)))
				.keyBy(r -> r.rideId);

		DataStream<TaxiFare> fareData = env
				.addSource(fareSourceOrTest(new TaxiFareSource(fareSourceFile, delayThreshold, throughputFactor)))
				.keyBy(f -> f.rideId);

		SingleOutputStreamOperator joinResult = rideData
				.connect(fareData)
				.process(new TimedEnrichmentFunction());

		// Печать несовпавших оплат из бокового потока
		printOrTest(joinResult.getSideOutput(lonelyFares));

		env.execute("Expiring State Job");
	}

	public static class TimedEnrichmentFunction extends KeyedCoProcessFunction<Long, TaxiRide, TaxiFare, Tuple2<TaxiRide, TaxiFare>> {
		private ValueState<TaxiRide> rideRecord;
		private ValueState<TaxiFare> fareRecord;

		@Override
		public void open(Configuration config) {
			rideRecord = getRuntimeContext().getState(new ValueStateDescriptor<>("ride-record", TaxiRide.class));
			fareRecord = getRuntimeContext().getState(new ValueStateDescriptor<>("fare-record", TaxiFare.class));
		}

		@Override
		public void processElement1(TaxiRide ride, Context ctx, Collector<Tuple2<TaxiRide, TaxiFare>> out) throws Exception {
			TaxiFare fare = fareRecord.value();
			if (fare != null) {
				fareRecord.clear();
				ctx.timerService().deleteEventTimeTimer(fare.getEventTime());
				out.collect(new Tuple2<>(ride, fare));
			} else {
				rideRecord.update(ride);
				ctx.timerService().registerEventTimeTimer(ride.getEventTime());
			}
		}

		@Override
		public void processElement2(TaxiFare fare, Context ctx, Collector<Tuple2<TaxiRide, TaxiFare>> out) throws Exception {
			TaxiRide ride = rideRecord.value();
			if (ride != null) {
				rideRecord.clear();
				ctx.timerService().deleteEventTimeTimer(ride.getEventTime());
				out.collect(new Tuple2<>(ride, fare));
			} else {
				fareRecord.update(fare);
				ctx.timerService().registerEventTimeTimer(fare.getEventTime());
			}
		}

		@Override
		public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple2<TaxiRide, TaxiFare>> out) throws Exception {
			if (fareRecord.value() != null) {
				ctx.output(lonelyFares, fareRecord.value());
				fareRecord.clear();
			}
			if (rideRecord.value() != null) {
				ctx.output(lonelyRides, rideRecord.value());
				rideRecord.clear();
			}
		}
	}
}