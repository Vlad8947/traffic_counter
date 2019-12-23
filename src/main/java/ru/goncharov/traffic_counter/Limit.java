package ru.goncharov.traffic_counter;

import java.io.Closeable;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Timer;
import java.util.TimerTask;

import static ru.goncharov.traffic_counter.Constant.*;

public class Limit implements Closeable {

    private int minLim;
    private int maxLim;
    private Connection connection; // Sql-connection
    // Timer for refresh limits
    private Timer refreshLimitsTimer = new Timer(true);

    public Limit(Connection connection) {
        this.connection = connection;
        // Get limits from data base
        refresh();

        // Start timer for refresh limits
        startRefreshTimer();
    }

    private void startRefreshTimer() {
        int period = 20*60*1000; // 20 minutes
        TimerTask timerTask = new TimerTask() {
            @Override
            public void run() {
                refresh();
            }
        };
        refreshLimitsTimer.scheduleAtFixedRate(timerTask, 0, period);
    }

    public void refresh() {
        try (Statement statement = connection.createStatement()) {

            ResultSet resultSet = statement.executeQuery(
                    "SELECT limit_name, limit_value " +
                            "FROM limits_per_hour " +
                            "WHERE limit_name = 'min' " +
                            "AND effective_date = " +
                            "(SELECT max(effective_date) FROM limits_per_hour WHERE limit_name = 'min') " +
                            "OR limit_name = 'max' " +
                            "AND effective_date = " +
                            "(SELECT max(effective_date) FROM limits_per_hour WHERE limit_name = 'max') " +
                            "LIMIT 2.");

            while (resultSet.next()) {
                if (MIN.equals(resultSet.getString(LIMIT_NAME))) {
                    minLim = resultSet.getInt(LIMIT_VALUE);
                } else {
                    maxLim = resultSet.getInt(LIMIT_VALUE);
                }
            }

        } catch (SQLException ex) {
            ex.printStackTrace();
        }
    }

    @Override
    public void close() throws IOException {
        refreshLimitsTimer.cancel();
    }

    // ----Getters/Setters-------------------------

    public int getMinLim() {
        return minLim;
    }

    public int getMaxLim() {
        return maxLim;
    }

}
