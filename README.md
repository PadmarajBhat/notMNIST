# notMNIST
Exploring notMNIST Dataset through SCALA



##### First Step is to untar the file using python
```
import tarfile
import sys
filename = r"notMNIST_small.tar.gz"
tar = tarfile.open(filename)
sys.stdout.flush()
tar.extractall()
tar.close()
```

##### install scala through sbt
  * through "sbt console" tried basics of scala syntax
  
##### IntelliJ installation for easy project management
 * post installation search for plugins in help tab and install scala.
