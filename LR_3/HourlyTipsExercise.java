package com.ververica.flinktraining.exercises.datastream_java.windows;

import com.ververica.flinktraining.exercises.datastream_java.datatypes.TaxiFare;
import com.ververica.flinktraining.exercises.datastream_java.sources.TaxiFareSource;
import com.ververica.flinktraining.exercises.datastream_java.utils.ExerciseBase;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * Расчет почасовых чаевых: нахождение водителя с максимальной суммой чаевых за каждый час.
 */
public class HourlyTipsExercise extends ExerciseBase {

	public static void main(String[] args) throws Exception {

		ParameterTool argumentParams = ParameterTool.fromArgs(args);
		final String fareInput = argumentParams.get("input", ExerciseBase.pathToFareData);

		final int maxDelaySec = 60;
		final int speedFactor = 600;

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		env.setParallelism(ExerciseBase.parallelism);

		DataStream<TaxiFare> fareStream = env.addSource(fareSourceOrTest(new TaxiFareSource(fareInput, maxDelaySec, speedFactor)));

		// Группировка по водителю, суммирование в часовом окне и поиск максимума
		DataStream<Tuple3<Long, Long, Float>> maxTipsPerHour = fareStream
				.keyBy((TaxiFare fare) -> fare.driverId)
				.timeWindow(Time.hours(1))
				.process(new TipSumCalculator())
				.timeWindowAll(Time.hours(1))
				.maxBy(2);

		printOrTest(maxTipsPerHour);

		env.execute("Hourly Tips Aggregation");
	}

	public static class TipSumCalculator extends ProcessWindowFunction<
			TaxiFare, Tuple3<Long, Long, Float>, Long, TimeWindow> {
		@Override
		public void process(Long driverId, Context ctx, Iterable<TaxiFare> fares, Collector<Tuple3<Long, Long, Float>> out) throws Exception {
			float totalTips = 0.0f;
			for (TaxiFare f : fares) {
				totalTips += f.tip;
			}
			// Вывод: (Конец_окна, ID_водителя, Сумма_чаевых)
			out.collect(new Tuple3<>(ctx.window().getEnd(), driverId, totalTips));
		}
	}
}