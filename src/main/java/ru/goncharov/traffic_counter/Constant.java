package ru.goncharov.traffic_counter;

public class Constant {

    public static final String APP_Name = "traffic_counter";

    public static final String DB_USER_NAME = "postgres";
    public static final String DB_PASSWORD = "0000";
    public static final String DB_NAME = "traffic_limits";
    public static final String DT_NAME = "limits_per_hour";
    public static final String DB_URL =
            "jdbc:postgresql://localhost:5432/" + DB_NAME + "?useLegacyDatetimeCode=false&amp&serverTimezone=UTC";

    public static final String MIN = "min";
    public static final String MAX = "max";

    public static final String LIMIT_NAME = "limit_name";
    public static final String LIMIT_VALUE = "limit_value";
    public static final String EFFECTIVE_DATE = "effective_date";

    public static final String ALERT_TOPIC = "alerts";
    public static final String CONSUMER_ADDRESS = "localhost:9092";
    public static final String PRODUCER_ADDRESS = "localhost:9092";

    public static final String MIN_LIMIT_MESSAGE = "Minimum limit reached";
    public static final String MAX_LIMIT_MESSAGE = "Maximum limit reached";

}
