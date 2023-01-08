# Miguel's Assingment For Quantexa

The assingment is, based on two different files sources, expecting to print some flight analytics:

  - Total number of flights for each month
  - Names of the 100 most frequent flyers
  - Greatest number of countries a passenger has been in without being in the UK
  - Passengers who have been on more than N flights together
  - Passengers who have been on more than N flights together within the range (from,to)

# Pre requirements
If cloned, code can be launched without any pre requirements.
Otherwise, it is required to place the flight related files into the main resources directory so the process can access them.

# Launch the process

As an Apache Spark process, it can be trigger with the `spark-submit` command.
For this process, it has been used the Spark version `2.3.4`.
In order to make the process easier to use, it has been declared one mandatory parameter: the directory mentioned above.
As so, it is required to package the whole code into a single .jar file:

```sh
$ cd /path/to/pom.xml
$ sbt 
```

With the previous maven command, the .jar file should have been created.
Next step is just running the process with the `spark-submit` command.

```sh
$ spark-submit --class quantexa.interview --master local /path/to/jar/directory/QuantexaInterview-0.0.1-SNAPSHOT.jar
```

In order to valdiate the results, all the expected outputs will be printed as a terminal ouput.

  - The result should be as follows:
![Expected Final Result](https://github.com/morbvel/AsosAsingment/blob/master/result.png)