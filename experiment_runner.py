from subprocess import STDOUT, check_output
import shutil

sets=["223","1124","221","1732","2697","48","193","1997","2586","2740","522","1501","721","2581","2907","1562"]
batch_sizes=["5000"]
for batch in batch_sizes:
    for set in sets:
        uri= "https://resourcesync.core.ac.uk/sitemaps/repo"+set+"/metadata/capabilitylist.xml"
        #print("removing target")
        #shutil.rmtree("/Users/mc26486/workspace/KMI/rs-aggregator/target/destination/resourcesync.core.ac.uk")
        #cmd = "java -cp rs-aggregator-jar-with-dependencies.jar uk.ac.core.main.CORESyncApp  --uri="+uri+" --measure --max=5000"
        cmd = "java -cp rs-aggregator-jar-with-dependencies.jar uk.ac.core.main.COREBatchSyncApp  --uri="+uri+" --batch-size="+batch+" --measure --max=5000"

        print(cmd)
        check_output(cmd,shell=True )


