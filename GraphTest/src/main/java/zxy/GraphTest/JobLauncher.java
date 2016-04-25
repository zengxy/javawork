package zxy.GraphTest;

import com.aliyun.odps.Odps;
import com.aliyun.odps.account.Account;
import com.aliyun.odps.account.AliyunAccount;
import com.aliyun.odps.data.TableInfo;
import com.aliyun.odps.graph.*;
import com.aliyun.odps.mapred.conf.SessionState;
import org.apache.commons.digester3.Digester;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.time.DateUtils;

import java.io.InputStream;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@SuppressWarnings("rawtypes")
public class JobLauncher {
    /**
     * 表示ODPS表的pojo类
     */
    public static class OdpsTableInfo {
        private String name;
        private List<String> partitions = new ArrayList<String>();

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public List<String> getPartitions() {
            return partitions;
        }

        public void setPartitions(List<String> partitions) {
            this.partitions = partitions;
        }

        public void addPartition(String partition) {
            partitions.add(partition);
        }
    }

    /**
     * 表示graph程序配置的pojo类
     */
    public static class GraphConfigInfo {
        //context
        private String baseId;
        private String projectId;
        private String resourceName;
        private String idePath;

        //class name
        //job.setGraphLoaderClass(cls); Graph Worker 载入图时会使用用户自定义的 {@link GraphLoader} 解析表记录为图的点或边。
        private String graphLoaderCls;
        //job.setCombinerClass(cls);
        private String combinerCls;
        //job.setAggregatorClass(cls); 设置多个 Aggregator 的实现类. 用分号分割
        private String aggregatorCls;
        //job.setPartitionerClass(cls);
        private String partitionerCls;
        //job.setVertexClass(cls); 设置 Vertex 的实现.
        private String vertexCls;
        //job.setWorkerComputerClass(cls); WorkerComputer 允许在 Worker 开始和退出时执行用户自定义的逻辑.
        private String workerComputerCls;
        //是否运行时对图进行重新分片
        private String runtimePartitioning = "false";
        //设置图加载阶段的冲突处理实现类，继承 {@link VertexResolver}.
        private String loadingVertexResolverCls;
        //设置图迭代计算阶段的冲突处理实现类，继承 {@link VertexResolver}.
        private String computingVertexResolverCls;

        /**
         * 置存储checkpoint的频率
         * 默认不设置表示按系统默认的方式做checkpoint，即间隔10分钟做一次checkpoint，如果设置，合法值是大于或等于0的整数，
         * 0表示不进行checkpoint，大于0，表示期望每隔n轮superstep做checkpoint，若n次superstep的时间小于系统checkpoint
         * 时间间隔（10分钟），也仍然等到10分钟后再做checkpoint.
         */
        private int checkPointSuperstepFrequency = 10;
        /**
         * 设置是否在Resolve与Compute之间同步
         * <p>
         * 当设置了ComputingVertexResolver后
         * Resolve和Compute之间会有统计信息的变更(totalVertxNum, totalEdgeNum等）
         * 需要多一轮sync操作，当确认自己对这些信息不敏感时，可以选择关掉
         * 默认打开sync
         * </p>
         *
         * @param is_sync 是否在Resolve与Compute之间同步
         */
        private String syncBetweenResolveCompute = "true";

        private String jobLauncherCls;

        //input and outpus
        private OdpsTableInfo inputTable;
        private OdpsTableInfo outputTable;

        //args
        private int maxIteration;

        private int numWorker;
        /**
         * 设置垃圾回收后开始使用disk-backed的内存阀值，默认0.5。
         *
         * @param threshold 内存阀值
         */
        private float memoryThreshold = 0.5f;
        /**
         * 设置是否开启disk-backed message，如果开启，则在内存到达阀值后尝试使用磁盘存储收到的message，默认关闭。
         *
         * @param useDiskBackedMessage  是否开启disk-backed message
         */
        private String useDiskBackedMessage = "false";
        /**
         * 设置是否开启disk-backed mutation，如果开启，则在内存到达阀值后尝试使用磁盘存储收到的mutation，默认关闭。
         *
         * @param useDiskBackedMutation 是否开启disk-backed mutation
         */
        private String useDiskBackedMutation = "false";

        //获取指定的切分大小，单位 MB，默认 64.
        private int splitSize;
        //设置 Worker 内存，单位MB，默认 4096.
        private int workerMem;
        //获取 Worker CPU，默认 200，表示两个 CPU 核.
        private int workerCpu;
        //int [0, 9]
        private int jobPriority;


        public String getBaseId() {
            return baseId;
        }

        public void setBaseId(String baseId) {
            this.baseId = baseId;
        }

        public String getProjectId() {
            return projectId;
        }

        public void setProjectId(String projectId) {
            this.projectId = projectId;
        }

        public String getGraphLoaderCls() {
            return graphLoaderCls;
        }

        public void setGraphLoaderCls(String graphLoaderCls) {
            this.graphLoaderCls = graphLoaderCls;
        }

        public String getCombinerCls() {
            return combinerCls;
        }

        public void setCombinerCls(String combinerCls) {
            this.combinerCls = combinerCls;
        }

        public String getAggregatorCls() {
            return aggregatorCls;
        }

        public void setAggregatorCls(String aggregatorCls) {
            this.aggregatorCls = aggregatorCls;
        }

        public String getPartitionerCls() {
            return partitionerCls;
        }

        public void setPartitionerCls(String partitionerCls) {
            this.partitionerCls = partitionerCls;
        }

        public String getVertexCls() {
            return vertexCls;
        }

        public void setVertexCls(String vertexCls) {
            this.vertexCls = vertexCls;
        }

        public String getWorkerComputerCls() {
            return workerComputerCls;
        }

        public void setWorkerComputerCls(String workerComputerCls) {
            this.workerComputerCls = workerComputerCls;
        }

        public String getLoadingVertexResolverCls() {
            return loadingVertexResolverCls;
        }

        public void setLoadingVertexResolverCls(String loadingVertexResolverCls) {
            this.loadingVertexResolverCls = loadingVertexResolverCls;
        }

        public String getComputingVertexResolverCls() {
            return computingVertexResolverCls;
        }

        public void setComputingVertexResolverCls(String computingVertexResolverCls) {
            this.computingVertexResolverCls = computingVertexResolverCls;
        }

        public String getResourceName() {
            return resourceName;
        }

        public void setResourceName(String resourceName) {
            this.resourceName = resourceName;
        }

        public String getIdePath() {
            return idePath;
        }

        public void setIdePath(String idePath) {
            this.idePath = idePath;
        }

        public OdpsTableInfo getInputTable() {
            return inputTable;
        }

        public void setInputTables(OdpsTableInfo inputTable) {
            this.inputTable = inputTable;
        }

        public OdpsTableInfo getOutputTable() {
            return outputTable;
        }

        public void setOutputTable(OdpsTableInfo outputTable) {
            this.outputTable = outputTable;
        }

        public int getCheckPointSuperstepFrequency() {
            return checkPointSuperstepFrequency;
        }

        public void setCheckPointSuperstepFrequency(int checkPointSuperstepFrequency) {
            this.checkPointSuperstepFrequency = checkPointSuperstepFrequency;
        }

        public int getMaxIteration() {
            return maxIteration;
        }

        public void setMaxIteration(int maxIteration) {
            this.maxIteration = maxIteration;
        }

        public int getNumWorker() {
            return numWorker;
        }

        public void setNumWorker(int numWorker) {
            this.numWorker = numWorker;
        }

        public float getMemoryThreshold() {
            return memoryThreshold;
        }

        public void setMemoryThreshold(float memoryThreshold) {
            this.memoryThreshold = memoryThreshold;
        }

        public int getSplitSize() {
            return splitSize;
        }

        public void setSplitSize(int splitSize) {
            this.splitSize = splitSize;
        }

        public int getWorkerMem() {
            return workerMem;
        }

        public void setWorkerMem(int workerMem) {
            this.workerMem = workerMem;
        }

        public int getWorkerCpu() {
            return workerCpu;
        }

        public void setWorkerCpu(int workerCpu) {
            this.workerCpu = workerCpu;
        }

        public int getJobPriority() {
            return jobPriority;
        }

        public void setJobPriority(int jobPriority) {
            this.jobPriority = jobPriority;
        }

        public void setInputTable(OdpsTableInfo inputTable) {
            this.inputTable = inputTable;
        }

        public String getJobLauncherCls() {
            return jobLauncherCls;
        }

        public void setJobLauncherCls(String jobLauncherCls) {
            this.jobLauncherCls = jobLauncherCls;
        }

        public String getRuntimePartitioning() {
            return runtimePartitioning;
        }

        public void setRuntimePartitioning(String runtimePartitioning) {
            this.runtimePartitioning = runtimePartitioning;
        }

        public String getSyncBetweenResolveCompute() {
            return syncBetweenResolveCompute;
        }

        public void setSyncBetweenResolveCompute(String syncBetweenResolveCompute) {
            this.syncBetweenResolveCompute = syncBetweenResolveCompute;
        }

        public String getUseDiskBackedMessage() {
            return useDiskBackedMessage;
        }

        public void setUseDiskBackedMessage(String useDiskBackedMessage) {
            this.useDiskBackedMessage = useDiskBackedMessage;
        }

        public String getUseDiskBackedMutation() {
            return useDiskBackedMutation;
        }

        public void setUseDiskBackedMutation(String useDiskBackedMutation) {
            this.useDiskBackedMutation = useDiskBackedMutation;
        }

    }

    /**
     * 以给定日期为基准展开宏
     *
     * @param str     含有待展开宏的字符串
     * @param dateYmd 基准日期，对应{yyyymmdd}，{yyyymmdd-n}，{yyyymmdd+n}
     * @return 展开后的字符串
     */
    public static String expandMacroDateYmd(String str, Date dateYmd) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
        Pattern dateYmdPat = Pattern.compile("\\{\\s*yyyymmdd\\s*(([+-])\\s*(\\d+))?\\s*\\}");
        String res = str;
        Matcher matcher = dateYmdPat.matcher(res);
        while (matcher.find()) {
            String expr = matcher.group(1);
            String op = matcher.group(2);
            String ndays = matcher.group(3);

            if (StringUtils.isEmpty(expr)) {
                // 无日期计算部分，直接展开
                res = matcher.replaceFirst(sdf.format(dateYmd));
                matcher = dateYmdPat.matcher(res);
            } else if ("+".equals(op)) {
                // 基准日期+n 天
                int n = Integer.parseInt(ndays);
                res = matcher.replaceFirst(sdf.format(DateUtils.addDays(dateYmd, n)));
                matcher = dateYmdPat.matcher(res);
            } else if ("-".equals(op)) {
                // 基准日期-n 天
                int n = -Integer.parseInt(ndays);
                res = matcher.replaceFirst(sdf.format(DateUtils.addDays(dateYmd, n)));
                matcher = dateYmdPat.matcher(res);
            }
        }
        return res;
    }

    /**
     * 解析base.graph.xml配置信息到java对象
     *
     * @return 配置信息pojo对象
     */
    public static GraphConfigInfo parseConfig(String extraPartitions) {
        Digester digester = new Digester();
        digester.setValidating(false);

        digester.addObjectCreate("graph", GraphConfigInfo.class);
        digester.addBeanPropertySetter("graph/baseId");
        digester.addBeanPropertySetter("graph/projectId");
        digester.addBeanPropertySetter("graph/resourceName");
        digester.addBeanPropertySetter("graph/idePath");

        digester.addBeanPropertySetter("graph/graphLoaderCls");
        digester.addBeanPropertySetter("graph/combinerCls");
        digester.addBeanPropertySetter("graph/aggregatorCls");
        digester.addBeanPropertySetter("graph/partitionerCls");
        digester.addBeanPropertySetter("graph/vertexCls");
        digester.addBeanPropertySetter("graph/workerComputerCls");
        digester.addBeanPropertySetter("graph/isUserTreeAggregator");
        digester.addBeanPropertySetter("graph/aggregatorTreeDepth");
        digester.addBeanPropertySetter("graph/runtimePartitioning");
        digester.addBeanPropertySetter("graph/loadingVertexResolverCls");
        digester.addBeanPropertySetter("graph/computingVertexResolverCls");

        digester.addBeanPropertySetter("graph/checkPointSuperstepFrequency");
        digester.addBeanPropertySetter("graph/syncBetweenResolveCompute");
        digester.addBeanPropertySetter("graph/jobLauncherCls");

        digester.addBeanPropertySetter("graph/maxIteration");
        digester.addBeanPropertySetter("graph/numWorker");

        digester.addBeanPropertySetter("graph/memoryThreshold");
        digester.addBeanPropertySetter("graph/useDiskBackedMessage");
        digester.addBeanPropertySetter("graph/useDiskBackedMutation");
        digester.addBeanPropertySetter("graph/broadcastMessageEnable");


        digester.addBeanPropertySetter("graph/splitSize");
        digester.addBeanPropertySetter("graph/workerMem");
        digester.addBeanPropertySetter("graph/workerCpu");

        digester.addBeanPropertySetter("graph/jobPriority");

        digester.addObjectCreate("graph/inputTable", OdpsTableInfo.class);
        digester.addBeanPropertySetter("graph/inputTable/name");
        digester.addCallMethod("graph/inputTable/partitions/partition", "addPartition", 1);
        digester.addCallParam("graph/inputTable/partitions/partition", 0);
        digester.addSetNext("graph/inputTable", "setInputTable");

        digester.addObjectCreate("graph/outputTable", OdpsTableInfo.class);
        digester.addBeanPropertySetter("graph/outputTable/name");
        digester.addCallMethod("graph/outputTable/partition", "addPartition", 1);
        digester.addCallParam("graph/outputTable/partition", 0);
        digester.addSetNext("graph/outputTable", "setOutputTable");

        InputStream is = ClassLoader.getSystemResourceAsStream("META-INF/base.graph.xml");
        try {
            GraphConfigInfo conf = digester.parse(is);

            // 将额外分区合并入输入表和输出表
            if (!extraPartitions.isEmpty()) {
                String[] eps = extraPartitions.split(":");
                for (String ep : eps) {
                    int pos = ep.indexOf("/");
                    String tableName = ep.substring(0, pos);
                    String partition = ep.substring(pos + 1);

                    if (conf.getInputTable().getName().equals(tableName)) {
                        conf.getInputTable().addPartition(partition);
                    }

                    if (conf.getOutputTable().getName().equals(tableName)) {
                        conf.getOutputTable().addPartition(partition);
                    }
                }
            }

            return conf;
        } catch (Exception e) {
            return null;
        }
    }

    /**
     * 通过配置和运行时参数构建JobConf
     *
     * @param conf    程序配置
     * @param dateYmd 基准时间
     * @return JobConf对象
     * @throws Exception
     */
    @SuppressWarnings("unchecked")
    public static GraphJob makeGraphJobConf(GraphConfigInfo conf, Date dateYmd) throws Exception {
        GraphJob job = new GraphJob();

        if (conf == null) {
            throw new Exception("Parse base.graph.xml failed!");
        }
        //强制配置 必须要有
        if (conf.getVertexCls() == null || conf.getVertexCls().isEmpty()) {
            throw new Exception("no graph computing class config exist!");
        }


        String vertexClsName = conf.getVertexCls();
        Class<Vertex> vertexCls = (Class<Vertex>) Class.forName(vertexClsName);
        job.setVertexClass(vertexCls);

        //强行配置 必须要有
        if (conf.getGraphLoaderCls() == null || conf.getGraphLoaderCls().isEmpty()) {
            throw new Exception("no vertex reader class config exist");
        }

        String graphLoaderClsName = conf.getGraphLoaderCls();
        Class<GraphLoader> graphLoaderCls = (Class<GraphLoader>) Class.forName(graphLoaderClsName);
        job.setGraphLoaderClass(graphLoaderCls);

        //选择配置
        if (conf.getCombinerCls() != null && !conf.getCombinerCls().isEmpty()) {
            String combinerClsName = conf.getCombinerCls();
            Class<Combiner> combinerCls = (Class<Combiner>) Class.forName(combinerClsName);
            job.setCombinerClass(combinerCls);
        }

        if (conf.getAggregatorCls() != null && !conf.getAggregatorCls().isEmpty()) {
            String[] aggregatorClsNames = conf.getAggregatorCls().split(";");
            Class<Aggregator>[] aggregatorClses = new Class[aggregatorClsNames.length];
            int index = 0;
            for (String aggregatorClsName : aggregatorClsNames) {
                aggregatorClses[index++] = (Class<Aggregator>) Class.forName(aggregatorClsName);
            }
            job.setAggregatorClass(aggregatorClses);
        }

        if (conf.getPartitionerCls() != null && !conf.getPartitionerCls().isEmpty()) {
            String partitionClsName = conf.getPartitionerCls();
            Class<Partitioner> partitionCls = (Class<Partitioner>) Class.forName(partitionClsName);
            job.setPartitionerClass(partitionCls);
        }

        if (conf.getWorkerComputerCls() != null && !conf.getWorkerComputerCls().isEmpty()) {
            String workComputerClsName = conf.getWorkerComputerCls();
            Class<WorkerComputer> workComputerCls = (Class<WorkerComputer>) Class.forName(workComputerClsName);
            job.setWorkerComputerClass(workComputerCls);
        }

        if (conf.getRuntimePartitioning() != null && !conf.getRuntimePartitioning().isEmpty()) {
            job.setRuntimePartitioning(Boolean.parseBoolean(conf.getRuntimePartitioning()));
        }

        if (conf.getLoadingVertexResolverCls() != null && !conf.getLoadingVertexResolverCls().isEmpty()) {
            String loadingVertexResolverClsName = conf.getLoadingVertexResolverCls();
            Class<VertexResolver> loadingVertexResolverCls = (Class<VertexResolver>) Class.forName(loadingVertexResolverClsName);
            job.setLoadingVertexResolverClass(loadingVertexResolverCls);
        }

        if (conf.getComputingVertexResolverCls() != null && !conf.getComputingVertexResolverCls().isEmpty()) {
            String computingVertexResolverClsName = conf.getComputingVertexResolverCls();
            Class<VertexResolver> computingVertexResolverCls = (Class<VertexResolver>) Class.forName(computingVertexResolverClsName);
            job.setComputingVertexResolverClass(computingVertexResolverCls);
        }

        if (conf.getCheckPointSuperstepFrequency() >= 0) {
            job.setCheckpointSuperstepFrequency(conf.getCheckPointSuperstepFrequency());
        }

        if (conf.getSyncBetweenResolveCompute() != null && !conf.getSyncBetweenResolveCompute().isEmpty()) {
            job.setSyncBetweenResolveCompute(Boolean.parseBoolean(conf.getSyncBetweenResolveCompute()));
        }

        if (conf.getMaxIteration() > 0) {
            job.setMaxIteration(conf.getMaxIteration());
        }

        if (conf.getNumWorker() > 0) {
            job.setNumWorkers(conf.getNumWorker());
        }

        if (conf.getMemoryThreshold() > 0.0) {
            job.setMemoryThreshold(conf.getMemoryThreshold());
        }
        if (conf.getUseDiskBackedMessage() != null && !conf.getUseDiskBackedMessage().isEmpty()) {
            job.setUseDiskBackedMessage(Boolean.parseBoolean(conf.getUseDiskBackedMessage()));
        }

        if (conf.getUseDiskBackedMutation() != null && !conf.getUseDiskBackedMutation().isEmpty()) {
            job.setUseDiskBackedMutation(Boolean.parseBoolean(conf.getUseDiskBackedMutation()));
        }

        if (conf.getSplitSize() > 0) {
            job.setSplitSize(conf.getSplitSize());
        }
        if (conf.getWorkerMem() > 0) {
            job.setWorkerMemory(conf.getWorkerMem());
        }
        if (conf.getWorkerCpu() > 0) {
            job.setWorkerCPU(conf.getWorkerCpu());
        }
        if (conf.getJobPriority() >= 0 && conf.getJobPriority() <= 9) {
            conf.setJobPriority(conf.getJobPriority());
        }

        // 设置输入表
        if (conf.getInputTable() == null) {
            throw new Exception("No input table specified");
        }
        if (conf.getInputTable().getPartitions() != null && conf.getInputTable().getPartitions().size() > 1) {
            throw new Exception("Input table can not has multiple partitions");
        }

        if (conf.getInputTable().getPartitions() == null || conf.getInputTable().getPartitions().size() == 0) {
            job.addInput(TableInfo.builder().tableName(conf.getInputTable().getName()).build());
        } else {
            job.addInput(TableInfo.builder().tableName(conf.getInputTable()
                    .getName())
                    .partSpec(expandMacroDateYmd(conf.getInputTable().getPartitions().get(0), dateYmd)).build());
        }

        // 设置输出表
        if (conf.getOutputTable() == null) {
            throw new Exception("No output table specified");
        }
        if (conf.getOutputTable().getPartitions() != null && conf.getOutputTable().getPartitions().size() > 1) {
            throw new Exception("Output table can not has multiple partitions");
        }

        if (conf.getOutputTable().getPartitions() == null || conf.getOutputTable().getPartitions().size() == 0) {
            job.addOutput(TableInfo.builder().tableName(conf.getOutputTable().getName()).build());
        } else {
            job.addOutput(TableInfo.builder().tableName(conf.getOutputTable()
                    .getName())
                    .partSpec(expandMacroDateYmd(conf.getOutputTable().getPartitions().get(0), dateYmd)).build());
        }

        return job;
    }

    public static void main(String[] args) throws Exception {
        String testDateYmd = "20140102";
        String extraPartitions = "";

        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
        GraphConfigInfo conf = parseConfig(extraPartitions);
        GraphJob job = makeGraphJobConf(conf, sdf.parse(testDateYmd));
        if (job == null) {
            throw new Exception("Create graph job failed");
        }

        Account account = new AliyunAccount("x", "x");
        Odps odps = new Odps(account);
        odps.setDefaultProject("local");
        SessionState sessionState = SessionState.get();
        sessionState.setLocalRun(true);
        sessionState.setOdps(odps);

        job.run();
    }
}
