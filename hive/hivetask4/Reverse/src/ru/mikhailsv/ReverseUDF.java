package ru.mikhailsv;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;

import java.lang.StringBuilder;

@Description(
        name = "reverse",
        value = "Returns reversed input string",
        extended = "Example:\n" +
                "SELECT reverse(field) from a;"
)
public class ReverseUDF extends UDF {

    public String evaluate(String str) {
        StringBuilder sb = new StringBuilder(str);  
        sb.reverse(); 
        return sb.toString();
    }
}
