# Resolução do case de Engenharia de dados

## Exercícios teóricos:

1. Qual o objetivo do comando cache em Spark?

O Spark tem sua lógica desenhada com Lazy Valuation, o comando cache força o Spark a executar o comando e retornar o conteúdo para a memória.

2. O mesmo código implementado em Spark é normalmente mais rápido que a implementação equivalente em MapReduce. Por quê?

O MapReduce roda em disco, o que garante um custo melhor e robustez no processamento. Já o Spark roda em memória, aumentando o custo por conta do hardware necessário mas tornando o processamento muito mais rápido e eficiente.

3. Qual a função do SparkContext?

O SparkContext é, literalmente, o contexto onde o Spark estará sendo executado. Rotineiramente apelidado de sc, ao criar uma conexão com o Spark, o sc recebe parâmetros de configuração que definem, por exemplo, o nome da conexão existente, além de permitir leitura de arquivos no HDFS, processamento de arquivos e algumas operações diretamente com RDDs. Comandos de manipulação de Spark DataFrames como o SparkSQL são executados diretamente na SparSession, que é comumente vista como a interface do Spark para o usuário.

Resumindo: Cluster > SparkContext > SparkSession

4. Explique com suas palavras o que é Resilient Distributed Datasets (RDD).

RDD é a principal estrutura de dados do Spark. Pensando em computação distribuída, o RDD já é lógicamente particionado para que o processamento seja facilmente computado em um cluster.

5. GroupByKey é menos eficiente que reduceByKey em grandes dataset. Por quê?

O GroupByKey realiza a agregação diretamente na working machine, causando tanto lentidão por transportar uma grande quantidade de dados via rede quanto problemas de disco ao armazenar toda essa informção na memória da máquina que realizará a agregação. O ReduceByKey aplica a agregação de forma distribuída retornando apenas os valores agregados de cada nó, restanto apenas a operação final da working machine para retornar a a agregação das saídas de cada um dos nós.

6. Explique o que o código Scala abaixo faz:

val textFile = sc.textFile("hdfs://...")

val counts = textFile.flatMap(line => line.split(" ")).map(word => (word, 1)).reduceByKey(_ + _)

counts.saveAsTextFile("hdfs://...")

O código cria uma variável textFile que aponta para um arquivo localizado no HDFS de um cluster.

A partir deste arquivo, uma variável counts é criada para retornar a contagem de palavras no texto. A função flatMap quebrará o arquivo em linhas e cada linha divida em palavras. Cada palavra se tornará uma tupla com o valor 1 que, no reduce, serão somados para cada palavra.

Por fim a variável counts é salva no HDFS como arquivo de texto.

## Exercícios práticos:

_O arquivo com os comandos utilizados está em src/main/scala/Nasa.scala_

1. Número de hosts únicos:

137933

2. O total de erros 404:

20901

3. Os 5 URLs que mais causaram erro 404.

HTTP | ERROS_404

GET /pub/winvn/readme.txt HTTP/1.0 | 2004

GET /pub/winvn/release.txt HTTP/1.0 | 1732 

GET /shuttle/missions/STS-69/mission-STS-69.html HTTP/1.0 | 682

GET /shuttle/missions/sts-68/ksc-upclose.gif HTTP/1.0 | 426

GET /history/apollo/a-001/a-001-patch-small.gif HTTP/1.0 | 384

4. Quantidade de erros 404 por dia:

A quantidade por dia está na pasta Output do projeto, mas por motivos de simplicidade trouxe a média de erros 404 por dia: 360

5. O total de bytes retornados:

65524314915


Para a resolução do case eu adaptei a classe disponível no git: https://github.com/alvinj/ScalaApacheAccessLogParser que foi desenvolvida para aplicação de regex nos logs de web servers. Sendo assim, modifiquei alguns tratamentos e reaproveitei o código para destinar esforços na análise dos dados.
