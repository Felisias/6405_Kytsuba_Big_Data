package com.ververica.flinktraining.exercises.datastream_java.basics;

import com.ververica.flinktraining.exercises.datastream_java.sources.TaxiRideSource;
import com.ververica.flinktraining.exercises.datastream_java.datatypes.TaxiRide;
import com.ververica.flinktraining.exercises.datastream_java.utils.ExerciseBase;
import com.ververica.flinktraining.exercises.datastream_java.utils.GeoUtils;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Упражнение по очистке данных: фильтрация поездок такси.
 * Необходимо оставить только те записи, где начало и конец поездки находятся в пределах Нью-Йорка.
 */
public class RideCleansingExercise extends ExerciseBase {
    public static void main(String[] args) throws Exception {

        ParameterTool params = ParameterTool.fromArgs(args);
        final String inputData = params.get("input", ExerciseBase.pathToRideData);

        // Конфигурация задержки и скорости воспроизведения событий
        final int eventDelay = 60;
        final int playbackSpeed = 600;

        StreamExecutionEnvironment executionEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        executionEnv.setParallelism(ExecutionConfig.PARALLELISM_DEFAULT);

        // Инициализация источника данных
        DataStream<TaxiRide> taxiRides = executionEnv.addSource(rideSourceOrTest(new TaxiRideSource(inputData, eventDelay, playbackSpeed)));

        // Применение фильтра для очистки потока
        DataStream<TaxiRide> nycRidesOnly = taxiRides
                .filter(new NewYorkCityFilter());

        printOrTest(nycRidesOnly);

        executionEnv.execute("Taxi Ride Cleansing Job");
    }

    /**
     * Пользовательский фильтр для проверки координат в пределах NYC.
     */
    public static class NewYorkCityFilter implements FilterFunction<TaxiRide> {
        @Override
        public boolean filter(TaxiRide ride) throws Exception {
            // Проверка начальной и конечной точки через утилиту GeoUtils
            return GeoUtils.isInNYC(ride.startLon, ride.startLat) &&
                    GeoUtils.isInNYC(ride.endLon, ride.endLat);
        }
    }
}