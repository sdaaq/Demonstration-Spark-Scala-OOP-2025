package com.example
package config

final case class OutputPaths(
                        topAirports: String,
                        topAirlines: String,
                        topDestinations: String,
                        topAirCarry: String,
                        topDays: String,
                        delayCounts: String,
                        delayPercentages: String
                      )


object OutputPaths {
  def default(dir: String = "output"): OutputPaths = OutputPaths(
    s"$dir/top_airports",
    s"$dir/top_airlines",
    s"$dir/top_destination_airport",
    s"$dir/topAirCarry",
    s"$dir/top_DayOfWeek",
    s"$dir/flights_count",
    s"$dir/timeDelay_percentage"
  )
}
