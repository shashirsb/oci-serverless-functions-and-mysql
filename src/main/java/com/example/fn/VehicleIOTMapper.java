package com.example.fn;

import org.apache.ibatis.annotations.Insert;
import org.apache.ibatis.annotations.Result;
import org.apache.ibatis.annotations.Results;
import org.apache.ibatis.annotations.Select;

import java.util.List;

public interface VehicleIOTMapper {

    String insertSql = "INSERT INTO veh_iot(COMPACTNESS, CIRCULARITY, DISTANCE_CIRCULARITY, RADIUS_RATIO, PR_AXIS_ASPECT_RATIO, MAX_LENGTH_ASPECT_RATIO, SCATTER_RATIO, ELONGATEDNESS, PR_AXIS_RECTANGULARITY, MAX_LENGTH_RECTANGULARITY, SCALED_VARIANCE_MAJOR, SCALED_VARIANCE_MINOR, SCALED_RADIUS_OF_GYRATION, SKEWNESS_ABOUT_MAJOR, SKEWNESS_ABOUT_MINOR, KURTOSIS_ABOUT_MAJOR, KURTOSIS_ABOUT_MINOR, HOLLOWS_RATIO, Category) values (#{COMPACTNESS},#{CIRCULARITY},#{DISTANCE_CIRCULARITY},#{RADIUS_RATIO},#{PR_AXIS_ASPECT_RATIO},#{MAX_LENGTH_ASPECT_RATIO},#{SCATTER_RATIO},#{ELONGATEDNESS},#{PR_AXIS_RECTANGULARITY},#{MAX_LENGTH_RECTANGULARITY},#{SCALED_VARIANCE_MAJOR},#{SCALED_VARIANCE_MINOR},#{SCALED_RADIUS_OF_GYRATION},#{SKEWNESS_ABOUT_MAJOR},#{SKEWNESS_ABOUT_MINOR},#{KURTOSIS_ABOUT_MAJOR},#{KURTOSIS_ABOUT_MINOR},#{HOLLOWS_RATIO},#{Category})";
    String selectAllSql = "SELECT * FROM veh_iot";

    @Insert(insertSql)
    Integer insert(VehicleIOT vehicleiot);

    @Select(selectAllSql)
    @Results(value = {            
        @Result(property = "COMPACTNESS", column = "COMPACTNESS"),
        @Result(property = "CIRCULARITY", column = "CIRCULARITY"),
        @Result(property = "DISTANCE_CIRCULARITY", column = "DISTANCE_CIRCULARITY"),
        @Result(property = "RADIUS_RATIO", column = "RADIUS_RATIO"),
        @Result(property = "PR_AXIS_ASPECT_RATIO", column = "PR_AXIS_ASPECT_RATIO"),
        @Result(property = "MAX_LENGTH_ASPECT_RATIO", column = "MAX_LENGTH_ASPECT_RATIO"),
        @Result(property = "SCATTER_RATIO", column = "SCATTER_RATIO"),
        @Result(property = "ELONGATEDNESS", column = "ELONGATEDNESS"),
        @Result(property = "PR_AXIS_RECTANGULARITY", column = "PR_AXIS_RECTANGULARITY"),
        @Result(property = "MAX_LENGTH_RECTANGULARITY", column = "MAX_LENGTH_RECTANGULARITY"),
        @Result(property = "SCALED_VARIANCE_MAJOR", column = "SCALED_VARIANCE_MAJOR"),
        @Result(property = "SCALED_VARIANCE_MINOR", column = "SCALED_VARIANCE_MINOR"),
        @Result(property = "SCALED_RADIUS_OF_GYRATION", column = "SCALED_RADIUS_OF_GYRATION"),
        @Result(property = "SKEWNESS_ABOUT_MAJOR", column = "SKEWNESS_ABOUT_MAJOR"),
        @Result(property = "SKEWNESS_ABOUT_MINOR", column = "SKEWNESS_ABOUT_MINOR"),
        @Result(property = "KURTOSIS_ABOUT_MAJOR", column = "KURTOSIS_ABOUT_MAJOR"),
        @Result(property = "KURTOSIS_ABOUT_MINOR", column = "KURTOSIS_ABOUT_MINOR"),
        @Result(property = "HOLLOWS_RATIO", column = "HOLLOWS_RATIO"),
        @Result(property = "Category", column = "Category")
    })
    List<VehicleIOT> selectAll();
}