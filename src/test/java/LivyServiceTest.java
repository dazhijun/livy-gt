import org.junit.jupiter.api.Test;
import space.jwqwy.livy.entiy.SparkJob;
import space.jwqwy.livy.eum.SparkJobState;
import space.jwqwy.livy.service.LivyService;
import space.jwqwy.livy.service.impl.LivyServiceImpl;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;


class LivyServiceTest {

    @Test
    void submitJob() {

        LivyService livyService = new LivyServiceImpl();

        SparkJob job = new SparkJob();

        job.setName("spark-examples");
        job.setFile("hdfs:/tmp/yansz/spark-examples_2.11-2.3.3.jar");
        job.setClassName("org.apache.spark.examples.SparkPi");
        job.setExecutorCores(1);


        int sparkJobID = livyService.startSparkJob(job);

        if (sparkJobID > 0) {
            System.out.println("\n创建任务，任务ID为：\n" + sparkJobID);

            Map<String, Object> activeSparkJobs = livyService.getActiveSparkJobs();
            System.out.println("\n查询当前所有任务：\n" + activeSparkJobs.toString());

            SparkJobState state = livyService.getSparkJobState(sparkJobID);
            System.out.println("\n查询任务ID为" + sparkJobID + "的任务状态:\n" + state);

            Map<String, Object> info = livyService.getSparkJobInfo(sparkJobID);
            System.out.println("\n查询任务ID为" + sparkJobID + "的任务详情:\n" + info.toString());

            String log = livyService.getSparkJobLog(sparkJobID);
            System.out.println("\n查询任务ID为" + sparkJobID + "的任务日志:\n" + log);

            // Map<String, Object> del = livyService.deleteSparkJob(sparkJobID);
            // System.out.println("删除任务ID为" + sparkJobID + "\n" + del.toString());
        }
        // 执行任务，一直到任务结束
        // System.out.println(runSparkJob(job));
    }


    @Test
    void findSession() {

        LivyService livyService = new LivyServiceImpl();

        SparkJob job = new SparkJob();


        job.setName("spark-examples");
        job.setFile("hdfs:/tmp/yansz/spark-examples_2.11-2.3.3.jar");
        job.setClassName("org.apache.spark.examples.SparkPi");
        job.setExecutorCores(1);


        int sparkJobID = livyService.startSparkJob(job);

        if (sparkJobID > 0) {
            System.out.println("\n创建任务，任务ID为：\n" + sparkJobID);

            Map<String, Object> activeSparkJobs = livyService.getActiveSparkJobs();
            System.out.println("\n查询当前所有任务：\n" + activeSparkJobs.toString());

            SparkJobState state = livyService.getSparkJobState(sparkJobID);
            System.out.println("\n查询任务ID为" + sparkJobID + "的任务状态:\n" + state);

            Map<String, Object> info = livyService.getSparkJobInfo(sparkJobID);
            System.out.println("\n查询任务ID为" + sparkJobID + "的任务详情:\n" + info.toString());

            String log = livyService.getSparkJobLog(sparkJobID);
            System.out.println("\n查询任务ID为" + sparkJobID + "的任务日志:\n" + log);

            // Map<String, Object> del = livyService.deleteSparkJob(sparkJobID);
            // System.out.println("删除任务ID为" + sparkJobID + "\n" + del.toString());
        }
        // 执行任务，一直到任务结束
        // System.out.println(runSparkJob(job));
    }


    /**
     * Livy查询 Spark 任务失败停用
     */
    @Test
    void failJobTest() {
        LivyService livyService = new LivyServiceImpl();
        SparkJob job = new SparkJob();
        job.setExecutorCores(3);
        List<String> jars = new ArrayList<>(1);
        job.setName("spark-examples");
        job.setFile("hdfs:/tmp/yansz/spark-examples_2.11-2.3.3.jar");
        job.setClassName("org.apache.spark.examples.SparkPi");
        job.setExecutorCores(1);

        int sparkJobID = livyService.runSparkJobBackground(job);

        while (true) {
            try {
                // 休眠3s
                Thread.sleep(4000);
            } catch (Exception ex) {
                ex.getMessage();
            }

            SparkJobState sparkJobState = livyService.getSparkJobState(sparkJobID);
            String log = livyService.getSparkJobLog(sparkJobID);

            System.out.println(log);

            switch (sparkJobState) {
                case SHUTTING_DOWN:
                    livyService.deleteSparkJob(sparkJobID);
                    return;
                case ERROR:
                    livyService.deleteSparkJob(sparkJobID);
                    return;
                case DEAD:
                    livyService.deleteSparkJob(sparkJobID);
                    return;
                case SUCCESS:
                    return;
                default:
            }

        }
    }

    @Test
    void sparkAnalysisTest() {
        LivyService livyService = new LivyServiceImpl();
        SparkJob job = new SparkJob();
        job.setName("spark-examples");
        job.setFile("hdfs:/tmp/yansz/spark-examples_2.11-2.3.3.jar");
        job.setClassName("org.apache.spark.examples.SparkPi");
        job.setExecutorCores(1);

        livyService.runSparkJob(job);
    }

}
