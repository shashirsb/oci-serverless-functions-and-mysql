package com.example.fn;

import org.apache.ibatis.annotations.Insert;
import org.apache.ibatis.annotations.Result;
import org.apache.ibatis.annotations.Results;
import org.apache.ibatis.annotations.Select;

import java.util.List;

public interface PersonMapper {

    String insertSql = "INSERT INTO person(id, first_name, last_name) values (#{id}, #{firstName}, #{lastName})";
    String selectAllSql = "SELECT * FROM person";

    @Insert(insertSql)
    Integer insert(Person person);

    @Select(selectAllSql)
    @Results(value = {
            @Result(property = "id", column = "ID"),
            @Result(property = "firstName", column = "FIRST_NAME"),
            @Result(property = "lastName", column = "LAST_NAME")
    })
    List<Person> selectAll();
}