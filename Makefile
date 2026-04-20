JDBC_JAR=mssql-jdbc-13.4.0.jre11.jar

build: futbolsoccer.class

futbolsoccer.class: futbolsoccer.java
	javac -cp ".;$(JDBC_JAR)" futbolsoccer.java

run: futbolsoccer.class
	java -cp ".;$(JDBC_JAR)" -Djava.library.path="." futbolsoccer

clean:
	del /Q futbolsoccer.class 2>nul