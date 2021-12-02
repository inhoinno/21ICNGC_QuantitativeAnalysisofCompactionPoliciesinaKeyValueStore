import os
from collections import defaultdict
DEV_PATH="../mnt"
RESULT_PATH="../result_txt"

testname = "quicktest_random_writeread"
''' 
 
 INPUT FILE 
  : bench
 OUTPUT FILE
  : RESULT_FILE, NAME(report), LOG




    % WATCH OUT %
     DONT PUT OPTIONS YOU DONT KNOW

      --key_prefix option make fillseq to fillrandom. 
      When workload is fillseq, write amplification of LVL should be 1.



'''

NUM= 25000000  #~10GB
#UM= 55000000  #~25GB
#UM=110000000  #~50GB
#UM=210000000  #100.13GB
#UM=1250000000 #596.04GB
RESULT_FILE="{0}_COMPACTIONSTYLE_BENCH_kKEY_vVALUE_cBLOCKCACHE_lLEVEL0_mMINMERGE_countCOUNT_rdonlyRDONLY.txt".format(testname)
OLDRESULT_FILE = RESULT_FILE

NAME="{0}_report_COMPACTIONSTYLE_BENCH_kKEY_vVALUE_cBLOCKCACHE_lLEVEL0_mMINMERGE_countCOUNT_rdonlyRDONLY.csv".format(testname)
OLDNAME=NAME

key_vars=[16]#,32,64,128,256,1024]
value_vars=[64]#,64,128,256,512,1024,4096]
cache_vars=[512] 
l0_vars = [4] # default : 4
min_merge_width_vars = [2] #default : 2
rdonly_vars=[1] # true , false
MEM_SIZE = 64*1024*1024
target_num = (256+256)*NUM

d = defaultdict(dict)
for k in key_vars:
    for v in value_vars:
        d[k][v] = int(target_num/(k+v))
print(k,v,target_num/(k+v))
#print(d[256][256])

compactionstyle=[0,1]
benchmark=["fillrandom","fillseq"]
#benchmark=["fillseq"]
I=0
bench=""
for count in range(0,1):
    for i in compactionstyle:
        for k in key_vars:
            for v in value_vars:
                for c in cache_vars:
                    for l in l0_vars:
                        for minmerge in min_merge_width_vars:
                            for rdonly in rdonly_vars:
                                for b in benchmark:

                                    BENCHMARK="{0},levelstats,stats".format(b)
                                    if i == 0 :
                                        RESULT_FILE = RESULT_FILE.replace("COMPACTIONSTYLE","LVL")
                                        NAME = NAME.replace("COMPACTIONSTYLE","LVL")
                                    elif i == 1 :
                                        RESULT_FILE = RESULT_FILE.replace("COMPACTIONSTYLE","UNIV")
                                        NAME=NAME.replace("COMPACTIONSTYLE","UNIV")
                                    RESULT_FILE = RESULT_FILE.replace("BENCH",b) 
                                    RESULT_FILE = RESULT_FILE.replace("KEY",str(k))
                                    RESULT_FILE = RESULT_FILE.replace("VALUE",str(v))
                                    RESULT_FILE = RESULT_FILE.replace("BLOCKCACHE",str(c))
                                    RESULT_FILE = RESULT_FILE.replace("LEVEL0",str(l))
                                    RESULT_FILE = RESULT_FILE.replace("MINMERGE",str(minmerge))
                                    RESULT_FILE = RESULT_FILE.replace("COUNT", str(count))
                                    RESULT_FILE = RESULT_FILE.replace("RDONLY",str(rdonly))

                                    NAME = NAME.replace("BENCH",b) 
                                    NAME = NAME.replace("KEY",str(k))
                                    NAME = NAME.replace("VALUE",str(v))
                                    NAME = NAME.replace("BLOCKCACHE",str(c))
                                    NAME = NAME.replace("LEVEL0",str(l))
                                    NAME = NAME.replace("MINMERGE",str(minmerge))
                                    NAME = NAME.replace("COUNT",str(count))
                                    NAME = NAME.replace("RDONLY",str(rdonly))


                                    C = int(c*1024*1024)
                                    if (b == "fillrandom") | (b=="fillseq"): 
                                        bench = "../db_bench --db=$DEV_PATH --use_direct_io_for_flush_and_compaction=true --benchmarks=$BENCHMARK --key_size=$k --value_size=$v --num=$NUM --compaction_style=$COMPACTIONSTYLE --histogram --statistics --cache_size=$c --level0_file_num_compaction_trigger=$l --universal_min_merge_width=$min_merge --report_interval_seconds=1 --report_file=$RESULT_PATH/$NAME >> $RESULT_PATH/$RESULT_FILE"

                                    bench = bench.replace("$DEV_PATH",DEV_PATH).replace("$BENCHMARK",BENCHMARK).replace("$k",str(k),2).replace("$v",str(v),2).replace("$NUM",str(d[k][v])).replace("$COMPACTIONSTYLE",str(i)).replace("$NAME",NAME).replace("$RESULT_PATH", RESULT_PATH,2).replace("$RESULT_FILE",RESULT_FILE).replace("$c",str(C)).replace("$l",str(l)).replace("$min_merge",str(minmerge))
                                    #print(bench)
                                    os.system("print > {0}/{1}".format(RESULT_PATH,RESULT_FILE))
                                    os.system("echo =========== {0} Compaction Style = {1} key size = {2}, value size = {3}, block cache size = {4} l0trigger {7} min_merge width {8} rdonly {9}=========== >>{5}/{6}".format(b,i,k,v,c,RESULT_PATH,RESULT_FILE,l,minmerge,rdonly))
                                    #os.system("iostat -cth 1 >> ../IOSTAT/iostat_cpu_{0} &".format(RESULT_FILE))
                                    #os.system("iostat -dxth 1 -p /dev/sdb2 >> ../IOSTAT/iostat_disk_{0} &".format(RESULT_FILE))
                                    os.system(bench)
                                    #print(bench)
                                    #print(I, RESULT_PATH+'/'+RESULT_FILE)
                                    os.system("cp ../mnt/LOG ../logs/LOG_{0}_COMPACTION_{1}_k{2}_v{3}_c{4}_l{5}_m{6}_count{7}_rdonly{8}".replace("COMPACTION","LVL" if i==0 else "UNIV").format(testname,b,k,v,c,l,minmerge,count,rdonly))

                                    RESULT_FILE=OLDRESULT_FILE
                                    NAME=OLDNAME
                                    I+=1


print("*********** bench FIN ****************")
os.system("python3 parser_integrated_L0_MINMERGE_RDONLY.py {0}".format(testname))
'''
    PARSER="../python_parser"
    PARSER_RESULT1="{0}_COMPACTIONSTYLE_BENCH_kKEY_vVALUE_cBLOCKCACHE.csv".format(testname)
    PARSER_RESULT2="{0}_99_COMPACTIONSTYLE_BENCH_kKEY_vVALUE_cBLOCKCACHE.csv".format(testname)
    PARSER_RESULT3="{0}_avg_COMPACTIONSTYLE_BENCH_kKEY_vVALUE_cBLOCKCACHE.csv".format(testname)
    PARSER_RESULT4="{0}_statistics_COMPACTIONSTYLE_BENCH_kKEY_vVALUE_cBLOCKCACHE.csv".format(testname)
    PARSER_RESULT5="{0}_perf_context_COMPACTIONSTYLE_BENCH_kKEY_vVALUE_cBLOCKCACHE.csv".format(testname)
    PARSER_RESULT6="{0}_WA_COMPACTIONSTYLE_BENCH_kKEY_vVALUE_cBLOCKCACHE.csv".format(testname)
    OLD1=PARSER_RESULT1
    OLD2=PARSER_RESULT2
    OLD3=PARSER_RESULT3
    OLD4=PARSER_RESULT4
    OLD5=PARSER_RESULT5
    OLD6=PARSER_RESULT6

    RESULT_FILE=OLDRESULT_FILE

    PARSER_RESULTS = [RESULT_FILE, PARSER_RESULT1, PARSER_RESULT2, PARSER_RESULT3, PARSER_RESULT4, PARSER_RESULT5]


    for i in compactionstyle:
    for k in key_vars:
    for v in value_vars:
    for c in cache_vars:
    for b in benchmark:
    BENCHMARK="{0},levelstats,stats".format(b)

    if i == 0 :
    PARSER_RESULT1 = PARSER_RESULT1.replace("COMPACTIONSTYLE","LVL")
    PARSER_RESULT2 = PARSER_RESULT2.replace("COMPACTIONSTYLE","LVL")
    PARSER_RESULT3 = PARSER_RESULT3.replace("COMPACTIONSTYLE","LVL")
    PARSER_RESULT4 = PARSER_RESULT4.replace("COMPACTIONSTYLE","LVL")
    PARSER_RESULT5 = PARSER_RESULT5.replace("COMPACTIONSTYLE","LVL")
    PARSER_RESULT6 = PARSER_RESULT6.replace("COMPACTIONSTYLE","LVL")

    RESULT_FILE = RESULT_FILE.replace("COMPACTIONSTYLE","LVL")
    elif i==1 :
    PARSER_RESULT1 = PARSER_RESULT1.replace("COMPACTIONSTYLE","UNIV")
    PARSER_RESULT2 = PARSER_RESULT2.replace("COMPACTIONSTYLE","UNIV")
    PARSER_RESULT3 = PARSER_RESULT3.replace("COMPACTIONSTYLE","UNIV")
    PARSER_RESULT4 = PARSER_RESULT4.replace("COMPACTIONSTYLE","UNIV")
    PARSER_RESULT5 = PARSER_RESULT5.replace("COMPACTIONSTYLE","UNIV")
    PARSER_RESULT6 = PARSER_RESULT6.replace("COMPACTIONSTYLE","LVL")

    RESULT_FILE = RESULT_FILE.replace("COMPACTIONSTYLE","UNIV")

    RESULT_FILE = RESULT_FILE.replace("KEY",str(k))
    RESULT_FILE = RESULT_FILE.replace("VALUE",str(v))
    RESULT_FILE = RESULT_FILE.replace("BLOCKCACHE",str(c))
#for p in PARSER_RESULTS : 
#p = p.replace("COMPACTIONSTYLE", "LVL" if i==0 else "UNIV" if i==1 else "FIFO")
#p = p.replace("KEY",str(k))
#p = p.replace("VALUE",str(v))
#p = p.replace("BLOCKCACHE",str(c))
#p = p.replace("BENCH", b)



    PARSER_RESULT1 = PARSER_RESULT1.replace("KEY",str(k))
    PARSER_RESULT1 = PARSER_RESULT1.replace("VALUE",str(v))
    PARSER_RESULT1 = PARSER_RESULT1.replace("BLOCKCACHE",str(c))
    PARSER_RESULT2 = PARSER_RESULT2.replace("KEY",str(k))
    PARSER_RESULT2 = PARSER_RESULT2.replace("VALUE",str(v))
    PARSER_RESULT2 = PARSER_RESULT2.replace("BLOCKCACHE",str(c))
    PARSER_RESULT3 = PARSER_RESULT3.replace("KEY",str(k))
    PARSER_RESULT3 = PARSER_RESULT3.replace("VALUE",str(v))
    PARSER_RESULT3 = PARSER_RESULT3.replace("BLOCKCACHE",str(c))

    PARSER_RESULT4 = PARSER_RESULT4.replace("KEY",str(k))
    PARSER_RESULT4 = PARSER_RESULT4.replace("VALUE",str(v))
    PARSER_RESULT4 = PARSER_RESULT4.replace("BLOCKCACHE",str(c))

    PARSER_RESULT5 = PARSER_RESULT5.replace("KEY",str(k))
    PARSER_RESULT5 = PARSER_RESULT5.replace("VALUE",str(v))
    PARSER_RESULT5 = PARSER_RESULT5.replace("BLOCKCACHE",str(c))

    PARSER_RESULT6 = PARSER_RESULT6.replace("KEY",str(k))
    PARSER_RESULT6 = PARSER_RESULT6.replace("VALUE",str(v))
    PARSER_RESULT6 = PARSER_RESULT6.replace("BLOCKCACHE",str(c))

    PARSER_RESULT1 = PARSER_RESULT1.replace("BENCH",b)
    PARSER_RESULT2 = PARSER_RESULT2.replace("BENCH",b)
    PARSER_RESULT3 = PARSER_RESULT3.replace("BENCH",b)
    PARSER_RESULT4 = PARSER_RESULT4.replace("BENCH",b)
    PARSER_RESULT5 = PARSER_RESULT5.replace("BENCH",b)
    PARSER_RESULT6 = PARSER_RESULT6.replace("BENCH",b)


    compaction_parser="python3 $PARSER/compaction_parser.py $RESULT_PATH/$RESULT_FILE $PARSER/csv/$PARSER_RESULT"
    latency_parser="python3 $PARSER/99parser.py $RESULT_PATH/$RESULT_FILE $PARSER/csv/$PARSER_RESULT"
    avg_perf_parser="python3 $PARSER/{0}_averagePerformance_parser.py $RESULT_PATH/$RESULT_FILE $PARSER/csv/$PARSER_RESULT".format(b)
    statistics_parser="python3 $PARSER/statistics_context_parser.py $RESULT_PATH/$RESULT_FILE $PARSER/csv/$PARSER_RESULT"
    perf_context_parser="python3 $PARSER/perf_context_parser.py $RESULT_PATH/$RESULT_FILE $PARSER/csv/$PARSER_RESULT"
    WA_parser="python3 $PARSER/WA_parser.py $RESULT_PATH/$RESULT_FILE $PARSER/csv/$PARSER_RESULT"


    compaction_parser=compaction_parser.replace("$PARSER", PARSER,2).replace("$RESULT_PATH",RESULT_PATH,2).replace("$RESULT_FILE",RESULT_FILE,2).replace("$PARSER_RESULT",PARSER_RESULT1)
    latency_parser=latency_parser.replace("$PARSER", PARSER,2).replace("$RESULT_PATH",RESULT_PATH,2).replace("$RESULT_FILE",RESULT_FILE,2).replace("$PARSER_RESULT",PARSER_RESULT2)
    avg_perf_parser=avg_perf_parser.replace("$PARSER", PARSER,2).replace("$RESULT_PATH",RESULT_PATH,2).replace("$RESULT_FILE",RESULT_FILE,2).replace("$PARSER_RESULT",PARSER_RESULT3)
    statistics_parser=statistics_parser.replace("$PARSER", PARSER,2).replace("$RESULT_PATH",RESULT_PATH,2).replace("$RESULT_FILE",RESULT_FILE,2).replace("$PARSER_RESULT",PARSER_RESULT4)
    perf_context_parser=perf_context_parser.replace("$PARSER", PARSER,2).replace("$RESULT_PATH",RESULT_PATH,2).replace("$RESULT_FILE",RESULT_FILE,2).replace("$PARSER_RESULT",PARSER_RESULT5)
    WA_parser=WA_parser.replace("$PARSER", PARSER,2).replace("$RESULT_PATH",RESULT_PATH,2).replace("$RESULT_FILE",RESULT_FILE,2).replace("$PARSER_RESULT",PARSER_RESULT6)

    os.system(compaction_parser)
    os.system(latency_parser)
    os.system(avg_perf_parser)
    os.system(statistics_parser)
    os.system(perf_context_parser)
os.system(WA_parser)

#print(compaction_parser)
#print(latency_parser)
#print(avg_perf_parser)

    RESULT_FILE=OLDRESULT_FILE
    PARSER_RESULT1=OLD1
    PARSER_RESULT2=OLD2
    PARSER_RESULT3=OLD3
    PARSER_RESULT4=OLD4
    PARSER_RESULT5=OLD5
    PARSER_RESULT6=OLD6

    print("*********** parser FIN ***************")
'''
