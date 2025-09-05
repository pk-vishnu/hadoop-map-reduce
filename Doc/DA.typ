#set page(
  paper: "us-letter",
  margin: (x:1.8cm, y:1.6cm),
  header: context if here().page()==1{""} else {align(right)[22BCE0511]}
)
#set text(
  font: "New Computer Modern",
  size: 14pt
)
#set par(
  justify: true,
  leading: 0.52em
)
#set heading(
  numbering:"1.1",  
)
#show link:underline
#place(
  top + center,
  float: true,
  scope: "parent",
  clearance: 2em,
)[
  #align(center, text(22pt)[
    *Digital Assignment 1* 
  ])
  #align(center, text(17pt)[
    *Case Study 1* - Low Video Completion Rates
  ])
  #grid(
    columns: (1fr, 1fr,1fr,1fr),
    align(center)[
      Big Data Analytics
      BCSE402L
    ],
    align(center)[
      F2/TF2
    ],
    align(center)[
      Vishnu P K
    ],
    align(center)[
      22BCE0511
    ],
  )
  #align(center)[
    *Synopsis* \
A video platform monitors video views with fields (video ID, user ID, duration watched). 
We explore how Map and Reduce functions can be used to identify videos with abnormally low 
completion rates.
  ]
]

= Set up Map Reduce Environment
A hadoop cluster is deployed using the official `apache/hadoop-3.4.1` image. The cluster is composed of 4 services as defined #link("https://github.com/pk-vishnu/hadoop-map-reduce/blob/main/docker-compose.yml")[here]. 

#figure(
  image("Images/HadoopContainers.png"),
  caption: "Hadoop Environment"
)<hadoopenv>

This provides a hadoop environment with one namenode, datanode, resourcemanager and nodemanager.

- A shared volume (./data:/data) is mounted on the Namenode container through which the data(video_data and map-red jar files) is transferred from host to HDFS.
- Cluster configuration is provided through a #link("https://github.com/pk-vishnu/hadoop-map-reduce/blob/main/config")[config file], defining core-site, hdfs-site, yarn-site, and mapred-site properties to ensure proper connectivity and job execution.

#pagebreak()

The Distributed File System (DFS) can now be accessed by bashing into the namenode's container and running `hdfs` commands.

#figure(
  image("Images/ResourceManagerWebUI.png"),
  caption: [DataNode information (Resource Manager WebUI on port 8088)]
)

#figure(
  image("Images/NameNodeUI.png"),
  caption: [Namenode (Namenode WebUI on port 9087)]
)
#pagebreak()


= Perform Map Tasks and Reduce Tasks
== Dataset Preparation
- #link("https://github.com/pk-vishnu/hadoop-map-reduce/blob/main/data/video_views_data.csv")[Video_Views_Dataset] contains `videoId`, `userId`, `durationWatched`, representing watch events of users across 6 videos.
- #link("https://github.com/pk-vishnu/hadoop-map-reduce/blob/main/data/video_metadata.csv")[Video_Metadata] contains `videoId`, `length` representing total duration of each video, which will be used to calculate the `completition rate` from the watch events.

#grid(
  columns: (1fr,1fr),
  rows: (auto),
  align(center)[
    #figure(
      image("Images/Video_Views.png"),
      caption: "Video watch events data"
    )<video_views_data>
  ],
  align(center)[
    #figure(
      image("Images/Video_Metadata.png"),
      caption: "Video metadata"
    )<video_views_data>
  ]
)

These files are placed in the shared volume `./data`, and then uploaded to the HDFS via bash.
 - `video_views_data.csv` is `put` into hdfs - `/input`
 - `video_metadata` is hardcoded has a hashmap in the reducer function (videoId - Length). In a real system, this data may be accessed via a central server/cache.

#pagebreak()

 #figure(
   image("Images/HDFS_upload.png" ),
   caption: "Create /input dir in HDFS and upload CSV from local FS"
 )

 #figure(
   image("Images/NamenodeWebUI.png"),
   caption: "File uploaded to DFS (NamenodeWebUI on port 9870)"
 )
#pagebreak()

== Map Reduce Job Setup 
=== #link("https://github.com/pk-vishnu/hadoop-map-reduce/blob/main/video_completion/src/main/java/VideoMapper.java")[Mapper Class]

*Mapper Function Logic*

Emit (`videoId` -> `durationWatched`) for each line in `video_views_data.csv`

*Input* \
Each line of the dataset. e.g `v001, u001, 43` (videoId, userId, durationWatched)

*Processing*\
+ Skip header
+ Split line with comma delimiter which gives a `String` array.
+ Extract `videoId`(arr[0]), `durationWatched`(arr[2]).

*Output*\
*Key*: Text(`Video Id`)
*Value*: IntWritable (`Video Duration in (sec)`)

#figure(
  image("Images/Mapper.png"),
  caption: [Mapper Class implementing Hadoop's base Mapper Class with above the logic @hadoop-mapper-api @macfarquhar-mapreduce-tutorial]
)

#pagebreak()

=== #link("https://github.com/pk-vishnu/hadoop-map-reduce/blob/main/video_completion/src/main/java/VideoReducer.java")[Reducer Class]

*Reducer Function Logic*

*Input* \
Key-Value pair from the Mapper Function

*Processing*\
+ Aggregate total watch time and calculate the number of views
+ Calculate
  $ 
  "Average Watch Duration" = "totalDuration" / "count"
  $ 
+ Look up actual Video Length from `video_metadata`. 
+ Calculate
  $
  "Completion Rate " = ("avgWatched"/"videoLength") * 100
  $
+ Check if completion rate < `Threshold` ($40%$)
*Output*\
Emit (videoId, Summary) if completion rate is abnormally low ($<40%$)

#figure(
  image("Images/Reducer.png", width: 93%),
  caption: [Reducer Class Implementing Hadoop's reducer class with above logic @hadoop-reducer-api @macfarquhar-mapreduce-tutorial]
)

#pagebreak()

=== #link("https://github.com/pk-vishnu/hadoop-map-reduce/blob/main/video_completion/src/main/java/VideoDriver.java")[Driver Class]
The `VideoDriver` Class has the driver program for the map-red workflow. It sets up the execution environment, configures the job and submits it to hadoop.

*Arguments to pass*
+ HDFS Input Path to `Video_Views_Dataset` CSV
+ HDFS Output Path to where results should be stored

#figure(
  image("Images/Driver.png"),
  caption: [Driver class that handles submitting the job to hadoop @hadoop-mapreduce-tutorial @macfarquhar-mapreduce-tutorial]
)

*Compilation*
The `POM.xml` with dependencies can be found #link("https://github.com/pk-vishnu/hadoop-map-reduce/blob/main/video_completion/pom.xml")[here].
#figure(
  image("Images/Compilation.png"),
  caption: [Build JAR with maven - mvn install (JDK1.8)]
)

#pagebreak()

== Running Job
After moving the compiled JAR to `./data`, the job is run by bashing into the Namenode container and running:

`
bash-4.2$: hadoop jar /data/MapReduce.jar VideoDriver /input/video_views_data.csv /output/completion"
`
#figure(
  image("Images/RunJob.png"),
  caption: "Run Job with `hadoop jar`"
)

The output is written into `/output/completion`

#figure(
  image("Images/Results.png"),
  caption: "Results of Map-Reduce"
)

*Inference*\
Out of the 6 videos v001, v002, v003, v004 and v006,  *`v001`* and *`v005`* have abnormally low completion rates below the set threshold ($40%$), indicating low engagement.

#figure(
  image("Images/TaskCompletion.png"),
  caption: [Task Status in Resource manager - Complete]
)

= GitHub Repository
#link("https://github.com/pk-vishnu/hadoop-map-reduce")[
MapReduce Java Code Hadoop_Cluster \ 
Hadoop Cluster(Compose YAML + Config)\
Typst Document
]

#bibliography("references.yaml")
