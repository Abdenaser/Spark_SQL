
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.max;

public class Application1 {
    public static void main(String[] args) {
        SparkSession ss = SparkSession.builder()
                .appName("Spark SQL")
                .master("local[*]").getOrCreate();

        Encoder<Employer> employeeBeanEncoder = Encoders.bean(Employer.class);
        Dataset<Employer> ds = ss.read().format("csv").option("delimiter",",")
                .option("header", true).option("charset", "UTF8")
                .option("inferSchema", "true").csv("Employes.csv")
                .as(employeeBeanEncoder);
        //Les employer ayant age 30 et 35
        System.out.println("Age entre 30 && 35");
        ds.filter((FilterFunction<Employer>) em -> em.getAge()>=30 && em.getAge()<=35).show();
        //Afficher la moyenne des salaires de chaque deparetemnt
        ds.groupBy("departement").mean("salary").show();
        ds.groupBy("departement").count().show();
        //le salaire maximum de tous les departements
        System.out.println("Max Salaire");
        ds.select(max("salary")).show();
    }
}