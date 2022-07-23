package org.njit.datamining;

import java.io.File;
import java.util.Scanner;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.ml.fpm.FPGrowth;
import org.apache.spark.ml.fpm.FPGrowthModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class DataMining {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		double userSupport; 
		double userConfidence;
		try {
			SparkSession spark = SparkSession.
					builder()
					.master(args[0])
					.appName("Data_Mining").getOrCreate();

			JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());
			jsc.setLogLevel("ERROR");

			@SuppressWarnings("resource")
			Scanner scanner = new Scanner(System.in);
			System.out.println("Welcome to Data Mining NJIT CS634 Summer 2022 : Dev :  Vivek Baskar");
			System.out.println("Final Term Project : Spark on EC2 - Association Rule Mining");

			System.out.println("Please enter Minimum Confidence in Multiples of 10: Max Value : 100 eg: 50 \n");
			String minConf = scanner.nextLine();
			userConfidence = Double.parseDouble(minConf);

			AssociationRule AssociationRule = new AssociationRule();
			System.out.println("\nPlease enter Minimum Support in Multiples of 10: Max Value : 100 eg: 50\n");
			String minSupport = scanner.nextLine(); 
			userSupport = Double.parseDouble(minSupport);

			StructType schema = DataTypes.createStructType(new StructField[] {
					DataTypes.createStructField("Item_1", DataTypes.StringType, true),
					DataTypes.createStructField("Item_2", DataTypes.StringType, true),
					DataTypes.createStructField("item_3", DataTypes.StringType, true),
					DataTypes.createStructField("Item_4", DataTypes.StringType, true)
			});

			File actual = new File(args[1]);
			for( File f : actual.listFiles()){
				if(f.isFile() && f.getName()!=null && f.getName().contains(".csv"))
				{	
					System.out.println("\n---------------------------------------------------------------------------------------");
					System.out.println("\nFile Name to be Processed :: "+f.getAbsoluteFile().getName());

					Dataset<Row> input = spark.read() 
							.schema(schema)
							.option("header", false)
							.option("delimiter",",")
							.option("nullValue","")
							.option("emptyValue","")
							.format("csv")
							.load(f.getAbsoluteFile().getName());
					System.out.println("Input Data:");
					input.show(30,false);

					input=input.na().fill("DEFAULT");
					//input.show(30,false);

					input.createOrReplaceTempView("input");
					System.out.println("Items Array Data:");

					Dataset<Row> itemsArrayDF = spark.sql("select array(Item_1,Item_2,Item_3,Item_4) as Items from input");
					//itemsArrayDF.show(30,false);

					itemsArrayDF=itemsArrayDF.withColumn("Items",functions.array_remove(functions.col("Items"), functions.lit("DEFAULT")));
					itemsArrayDF.show(30,false);

					//itemsArrayDF.printSchema();

					FPGrowthModel fpGrowthModel = new FPGrowth()
							.setItemsCol("Items")
							.setMinSupport(userSupport/100)
							.setMinConfidence(userConfidence/100)
							.fit(itemsArrayDF);

					System.out.println("Frequent Item Sets:");
					fpGrowthModel.freqItemsets().show(30,false);

					System.out.println("Association Rules:");
					fpGrowthModel.associationRules().show(30,false);

					System.out.println("Input items against all the Association Rules and Displayed as Consequents as prediction:");
					fpGrowthModel.transform(itemsArrayDF).show(30,false);
				}
			}
			spark.stop();
			System.exit(0);
		}
		catch(Exception e)
		{
			e.printStackTrace();
			System.exit(0);
		}
	}
}
//./bin/spark-submit  --class org.njit.datamining.DataMining  --master "spark://Viveks-MacBook-Pro.local:7077"  /Users/vivekbaskar/eclipse-workspace/CloudDataMiningEngine/target/DataMining-jar-with-dependencies.jar  /Users/vivekbaskar/eclipse-workspace/CloudDataMiningEngine/Dataset1.csv spark://Viveks-MacBook-Pro.local:7077

//./bin/spark-submit  --class org.njit.datamining.DataMining  --master "spark://Viveks-MacBook-Pro.local:7077"  /Users/vivekbaskar/eclipse-workspace/CloudDataMiningEngine/target/DataMining-jar-with-dependencies.jar  /Users/vivekbaskar/eclipse-workspace/CloudDataMiningEngine/Dataset7.csv spark://Viveks-MacBook-Pro.local:7077 0.5 0.6
