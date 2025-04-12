-- Databricks notebook source
-- DBTITLE 1,Importação de Dados e Criação da Camada Bronze
-- MAGIC %python 
-- MAGIC
-- MAGIC #Importando biblioteca 
-- MAGIC from pyspark.sql import SparkSession    
-- MAGIC                     
-- MAGIC # Criando a sessão Spark por segurança
-- MAGIC spark = SparkSession.builder.appName("DatabricksETL").getOrCreate()   
-- MAGIC     
-- MAGIC # Criando esquema Bronze caso não exista
-- MAGIC spark.sql("CREATE SCHEMA IF NOT EXISTS bronze")    
-- MAGIC
-- MAGIC # Carregando o dataset escolhido
-- MAGIC bronze_path = "dbfs:/FileStore/shared_uploads/doamaral.amanda@gmail.com/StudentPerfomanceFactors-1.csv" 
-- MAGIC
-- MAGIC #Lendo CSV e criando DataFrame Bronze
-- MAGIC df_bronze = (spark.read
-- MAGIC     .option("header", "true")
-- MAGIC     .option("inferSchema", "true")  
-- MAGIC     .option("sep", ",") #Definindo delimitador
-- MAGIC     .csv(bronze_path)) #Indicando o caminho do dataset
-- MAGIC
-- MAGIC # Criando tabela Bronze (Raw Data)
-- MAGIC df_bronze.write.format("delta").mode("overwrite").saveAsTable("bronze.student_performance")
-- MAGIC
-- MAGIC # Exibindo dados iniciais
-- MAGIC display(df_bronze)

-- COMMAND ----------

-- Salvar os dados traduzidos e com novos nomes em uma nova tabela "bronze.traducao_pt"
CREATE OR REPLACE TABLE bronze.traducao_pt
AS
SELECT
    Hours_Studied AS Horas_Estudadas,
    Attendance AS Frequencia,
    Parental_Involvement AS Suporte_Pais,
    Access_to_Resources AS Acesso_Recursos,
    Extracurricular_Activities AS Ativ_Extra,
    Sleep_Hours AS Qld_Sono,
    Previous_Scores AS Notas_Anteriores,
    Motivation_Level AS Motivacao,
    Internet_Access AS Acesso_Internet,
    Tutoring_Sessions AS Tutorias,
    Family_Income AS Sit_Financeira,
    Teacher_Quality AS Qld_Professores,
    School_Type AS Tipo_Escola,
    Peer_Influence AS Influencia,
    Physical_Activity AS Ativ_Fisica,
    Learning_Disabilities AS Dificuldade,
    Parental_Education_Level AS Educa_Pais,
    Distance_from_Home AS Escola_Casa,
    Gender AS Genero,
    Exam_Score AS Nota
FROM bronze.student_performance;


-- COMMAND ----------

-- Visualizando a nova tabela com campos em potuguês
SELECT *  
FROM bronze.traducao_pt;

-- COMMAND ----------

-- DBTITLE 1,Verificando tipo de dado para cada campo
DESCRIBE bronze.traducao_pt;

-- COMMAND ----------

-- DBTITLE 1,Domínio do campo Suporte_Pais
-- Verificando as categorias possíveis para campos categóricos
SELECT DISTINCT Suporte_Pais
FROM bronze.traducao_pt;

-- COMMAND ----------

SELECT DISTINCT Qld_Professores
FROM bronze.traducao_pt;

-- COMMAND ----------

-- DBTITLE 1,Domínio do Atributo Acesso_Recursos
SELECT DISTINCT Acesso_Recursos
FROM bronze.traducao_pt;

-- COMMAND ----------

-- DBTITLE 1,Domínio do Atributo Ativ_Extra
SELECT DISTINCT Ativ_Extra
FROM bronze.traducao_pt;

-- COMMAND ----------

-- DBTITLE 1,Domínio do Atributo Sit_Financeira
SELECT DISTINCT Sit_Financeira
FROM bronze.traducao_pt;

-- COMMAND ----------

-- DBTITLE 1,Domínio do Atributo Tipo_Escola
SELECT DISTINCT Tipo_Escola
FROM bronze.traducao_pt;

-- COMMAND ----------

-- DBTITLE 1,Domínio do Atributo Acesso_Internet
SELECT DISTINCT Acesso_Internet
FROM bronze.traducao_pt;

-- COMMAND ----------

-- DBTITLE 1,Domínio do Atributo Influencia
SELECT DISTINCT Influencia
FROM bronze.traducao_pt;

-- COMMAND ----------

-- DBTITLE 1,Domínio do Atributo Motivacao
SELECT DISTINCT Motivacao
FROM bronze.traducao_pt;

-- COMMAND ----------

-- DBTITLE 1,Domínio do Atributo Dificuldade
SELECT DISTINCT Dificuldade
FROM bronze.traducao_pt;

-- COMMAND ----------

-- DBTITLE 1,Domínio do Atributo Educa_pais
SELECT DISTINCT Educa_Pais
FROM bronze.traducao_pt;

-- COMMAND ----------

-- DBTITLE 1,Domínio do Atributo Escola_Casa
SELECT DISTINCT Escola_Casa
FROM bronze.traducao_pt;

-- COMMAND ----------

-- DBTITLE 1,Domínio do Atributo Genero
SELECT DISTINCT Genero
FROM bronze.traducao_pt;

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS prata;

-- COMMAND ----------

-- DBTITLE 1,Removendo valores Nulos que foram encontrados nos Atributos Categoricos
CREATE OR REPLACE TABLE prata.inicial AS
SELECT *
FROM bronze.traducao_pt
WHERE Qld_Professores IS NOT NULL
  AND Educa_Pais IS NOT NULL
  AND Escola_Casa IS NOT NULL;

-- COMMAND ----------

-- DBTITLE 1,Se certificando que os valores Nulos foram removidos de vez da tabela
SELECT *
FROM prata.inicial
WHERE Qld_Professores IS NULL 
   OR Educa_Pais IS NULL
   OR Escola_Casa IS NULL
   OR Nota IS NULL
   OR Horas_Estudadas IS NULL
   OR Tutorias IS NULL
   OR Frequencia IS NULL
   OR Notas_Anteriores IS NULL
   OR Genero IS NULL   
   OR Nota IS NULL
   OR Dificuldade IS NULL
   OR Influencia IS NULL
   OR Tipo_Escola IS NULL
   OR Sit_Financeira IS NULL
   OR Acesso_Internet IS NULL
   OR Acesso_Internet IS NULL
   OR Qld_Sono IS NULL   
   OR Ativ_Extra IS NULL
   OR Acesso_Recursos IS NULL
   OR Suporte_Pais IS NULL;

-- COMMAND ----------

-- DBTITLE 1,Busca por valores inválidos (Fora do domínio)
--Verificando valores inválidos para Nota
SELECT COUNT(*) AS invalidas
FROM prata.inicial
WHERE Nota < 0 OR Nota > 100 OR   
      Horas_Estudadas < 0 OR 
      Tutorias < 0 OR
      Ativ_Fisica < 0 OR
      Escola_Casa < 0 OR
      Frequencia < 0 OR Frequencia > 100 OR 
      Notas_Anteriores < 0 OR Notas_Anteriores > 100;

-- COMMAND ----------

-- DBTITLE 1,Identificando o valor inválido (Fora do domínio)
SELECT *
FROM prata.inicial
WHERE Nota < 0 OR Nota > 100 OR   
      Horas_Estudadas < 0 OR 
      Tutorias < 0 OR
      Ativ_Fisica < 0 OR
      Escola_Casa < 0 OR
      Frequencia < 0 OR Frequencia > 100 OR 
      Notas_Anteriores < 0 OR Notas_Anteriores > 100;


-- COMMAND ----------

-- DBTITLE 1,Removendo valores inválidos (Fora do Domínio)
-- Como encontrei um registro inválido para um dos campos, vou removê-lo
CREATE OR REPLACE TEMP VIEW prata_validos AS
SELECT *
FROM prata.inicial
WHERE 
    Nota BETWEEN 0 AND 100 AND
    Horas_Estudadas >= 0 AND
    Tutorias >= 0 AND
    Frequencia BETWEEN 0 AND 100 AND
    Notas_Anteriores BETWEEN 0 AND 100;

-- COMMAND ----------

-- DBTITLE 1,Verificando ausência de valores inválidos (Fora do domínio)
--Validando remoção de valores inválidos
SELECT *
FROM prata_validos
WHERE Nota < 0 OR Nota > 100 OR   
      Horas_Estudadas < 0 OR 
      Tutorias < 0 OR
      Frequencia < 0 AND Frequencia > 100 OR 
      Notas_Anteriores < 0 AND Notas_Anteriores > 100;

-- COMMAND ----------

-- DBTITLE 1,Verificando Domínio dos Dados Numéricos
-- Encontrando Máximos e Mínimos de Dados Numéricos desse Dataset  
SELECT MAX(Nota) AS Maximo_Nota, MIN(Nota) AS Minimo_Nota,  
MAX(Horas_Estudadas) AS Maximo_HEstudadas, MIN(Horas_Estudadas) AS Minimo_HEstudadas,   
MAX(Frequencia) AS Maximo_Frequencia, MIN(Frequencia) AS Minimo_Frequencia, 
MAX(Qld_Sono) AS Maximo_Sono, MIN(Qld_Sono) AS Minimo_Sono, 
MAX(Notas_Anteriores) AS Maximo_Notas_Ant, MIN(Notas_Anteriores) AS Minimo_Notas_Ant,
MAX(Tutorias) AS Maximo_Tutorias, MIN(Tutorias) AS Minimo_Tutorias,
MAX(Ativ_Fisica) AS Maximo_Ativ_Fisica, MIN(Ativ_Fisica) AS Minimo_Ativ_Fisica
FROM prata_validos;

-- COMMAND ----------

-- DBTITLE 1,Atualizando camada prata com dados limpos, sem espaço e em Caps Lock para unformização
-- Criar a camada Prata com dados limpos
CREATE OR REPLACE TEMP VIEW prata_limpos AS
SELECT 
    Horas_Estudadas,
    Frequencia,
    UPPER(TRIM(Suporte_Pais)) AS Suporte_Pais,
    UPPER(TRIM(Acesso_Recursos)) AS Acesso_Recursos,
    UPPER(TRIM(Ativ_Extra)) AS Ativ_Extra,
    Qld_Sono,
    Notas_Anteriores,
    UPPER(TRIM(Motivacao)) AS Motivacao,
    UPPER(TRIM(Acesso_Internet)) AS Acesso_Internet,
    Tutorias,
    UPPER(TRIM(Sit_Financeira)) AS Sit_Financeira,
    UPPER(TRIM(Qld_Professores)) AS Qld_Professores,
    UPPER(TRIM(Tipo_Escola)) AS Tipo_Escola,
    UPPER(TRIM(Influencia)) AS Influencia,
    Ativ_Fisica,
    UPPER(TRIM(Dificuldade)) AS Dificuldade,
    UPPER(TRIM(Educa_Pais)) AS Educa_Pais,
    UPPER(TRIM(Escola_Casa)) AS Escola_Casa,
    UPPER(TRIM(Genero)) AS Genero,
    Nota
FROM prata_validos;

-- COMMAND ----------

-- DBTITLE 1,Exibindo informações dos atributos tratados/uniformizados na camada Prata
SELECT *
FROM prata_limpos
LIMIT 10;

-- COMMAND ----------

-- DBTITLE 1,Criando nova coluna categorizando notas
CREATE OR REPLACE TABLE prata.nova_col AS 
SELECT *,
    CASE 
        WHEN Nota >= 85 THEN 'HIGH'
        WHEN Nota >= 70 AND Nota < 85 THEN 'MEDIUM'
        ELSE 'LOW' 
    END AS Categ_Nota
FROM prata_limpos;

-- COMMAND ----------

-- DBTITLE 1,Verificando inserção da nova coluna e seu tipo de dado
SELECT * 
FROM prata.nova_col
LIMIT 5;

-- COMMAND ----------

-- DBTITLE 1,Verificando número de linhas da tabela (quantidade de alunos analisados)
SELECT COUNT(*) 
FROM prata.nova_col;

-- COMMAND ----------

-- DBTITLE 1,Garantindo que não há alunos duplicados
SELECT COUNT(*) FROM (SELECT DISTINCT * FROM prata.nova_col) AS sub;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Criando tabelas para estruturar a análise utilizando Modelo Estrela

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC dbutils.fs.rm("dbfs:/user/hive/warehouse/prata.db/dim_aluno", recurse=True)
-- MAGIC

-- COMMAND ----------

-- DBTITLE 1,Criando tabelas para estruturar a análise utilizando Modelo Estrela
-- Criando Tabela para Dimensão de Aluno
CREATE OR REPLACE TABLE prata.Dim_Aluno (
    Genero STRING,
    Sit_Financeira STRING,
    Suporte_Pais STRING,
    Acesso_Recursos STRING,
    Acesso_Internet STRING,
    Educa_Pais STRING
) USING DELTA
LOCATION 'dbfs:/user/hive/warehouse/prata.db/dim_aluno';

-- Criando Tabela para Dimensão de Escola
CREATE OR REPLACE TABLE prata.Dim_Escola (
    Qld_Professores STRING,
    Notas_Anteriores INT,
    Tipo_Escola STRING,
    Escola_Casa STRING
) USING DELTA
LOCATION 'dbfs:/user/hive/warehouse/prata.db/dim_escola';

-- Criando Tabela para Dimensão de Estudo
CREATE OR REPLACE TABLE prata.Dim_Estudo (
    Dificuldade STRING,
    Frequencia INT,
    Tutorias INT,
    Horas_Estudadas INT
) USING DELTA
LOCATION 'dbfs:/user/hive/warehouse/prata.db/dim_estudo';

-- Criando Tabela para Dimensão de Comportamento
CREATE OR REPLACE TABLE prata.Dim_Comportamento (
    Ativ_Fisica INT,
    Qld_Sono INT,
    Ativ_Extra STRING,
    Motivacao STRING,
    Influencia STRING
) USING DELTA
LOCATION 'dbfs:/user/hive/warehouse/prata.db/dim_comportamento';

-- Criando a Tabela para Tabela Fato de Desempenho
CREATE OR REPLACE TABLE prata.Fato_Desempenho (
    Nota INT,
    Categ_Nota STRING,
    Id_Aluno INT,
    Id_Escola INT,
    Id_Estudo INT,
    Id_Comportamento INT
) USING DELTA
LOCATION 'dbfs:/user/hive/warehouse/prata.db/fato_desempenho';


-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS ouro;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Distribuição geral por categoria da nota (Categ_Nota)

-- COMMAND ----------

--Foi criada uma tabela com a distribuição das notas por categoria, incluindo seus %. A Tabela se encontra no catálogo do Databricks e na seção Objetivos & Estratégias no Notions
CREATE OR REPLACE TABLE ouro.dist_notas
SELECT Categ_Nota, 
COUNT(*) AS total, 
ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 1) AS porcentagem
FROM prata.nova_col
GROUP BY Categ_Nota;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC Criando chaves para correlação entre tabelas (monotonically_increasing_id()) e populando tabelas no modelo Estrela

-- COMMAND ----------

CREATE OR REPLACE TABLE ouro.Dim_Aluno AS
SELECT
  monotonically_increasing_id() AS Codigo,
  Genero,
  Sit_Financeira,
  Suporte_Pais,
  Acesso_Recursos,
  Acesso_Internet,
  Educa_Pais
FROM (
  SELECT DISTINCT
    Genero,
    Sit_Financeira,
    Suporte_Pais,
    Acesso_Recursos,
    Acesso_Internet,
    Educa_Pais
  FROM prata.nova_col
);


-- COMMAND ----------

CREATE OR REPLACE TABLE ouro.Dim_Escola AS
SELECT
  monotonically_increasing_id() AS Codigo,
  Qld_Professores,
  Notas_Anteriores,
  Tipo_Escola,
  Escola_Casa
FROM (
  SELECT DISTINCT
    Qld_Professores,
    Notas_Anteriores,
    Tipo_Escola,
    Escola_Casa
  FROM prata.nova_col
);


-- COMMAND ----------

CREATE OR REPLACE TABLE ouro.Dim_Estudo AS
SELECT
  monotonically_increasing_id() AS Codigo,
  Dificuldade,
  Frequencia,
  Tutorias,
  Horas_Estudadas
FROM (
  SELECT DISTINCT
    Dificuldade,
    Frequencia,
    Tutorias,
    Horas_Estudadas
  FROM prata.nova_col
);


-- COMMAND ----------

CREATE OR REPLACE TABLE ouro.Dim_Comportamento AS
SELECT
  monotonically_increasing_id() AS Codigo,
  Ativ_Fisica,
  Qld_Sono,
  Ativ_Extra,
  Motivacao,
  Influencia
FROM (
  SELECT DISTINCT
    Ativ_Fisica,
    Qld_Sono,
    Ativ_Extra,
    Motivacao,
    Influencia
  FROM prata.nova_col
);


-- COMMAND ----------

-- MAGIC %md
-- MAGIC Criando nova tabela fato (Ouro) com base nas tabelas dimensões que já estão com suas chaves primárias. Realizando a correlação PK-FK

-- COMMAND ----------

-- 
CREATE OR REPLACE TABLE ouro.Fato_Desempenho AS
SELECT
  f.Nota,
  a.Codigo AS Id_Aluno,
  e.Codigo AS Id_Escola,
  es.Codigo AS Id_Estudo,
  c.Codigo AS Id_Comportamento,
  f.Categ_Nota
FROM prata.nova_col f

JOIN ouro.Dim_Aluno a ON 
    f.Genero = a.Genero AND
    f.Sit_Financeira = a.Sit_Financeira AND
    f.Suporte_Pais = a.Suporte_Pais AND
    f.Acesso_Recursos = a.Acesso_Recursos AND
    f.Acesso_Internet = a.Acesso_Internet AND
    f.Educa_Pais = a.Educa_Pais

JOIN ouro.Dim_Escola e ON 
    f.Qld_Professores = e.Qld_Professores AND
    f.Notas_Anteriores = e.Notas_Anteriores AND
    f.Tipo_Escola = e.Tipo_Escola AND
    f.Escola_Casa = e.Escola_Casa

JOIN ouro.Dim_Estudo es ON 
    f.Dificuldade = es.Dificuldade AND
    f.Frequencia = es.Frequencia AND
    f.Tutorias = es.Tutorias AND
    f.Horas_Estudadas = es.Horas_Estudadas

JOIN ouro.Dim_Comportamento c ON 
    f.Ativ_Fisica = c.Ativ_Fisica AND
    f.Qld_Sono = c.Qld_Sono AND
    f.Ativ_Extra = c.Ativ_Extra AND
    f.Motivacao = c.Motivacao AND
    f.Influencia = c.Influencia;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC Qual a tendência para as variáveis quando os alunos tem o melhor desempenho (HIGH)?

-- COMMAND ----------

-- Verificando com o fator Motivação
SELECT c.Motivacao, COUNT(*) AS qtd
FROM ouro.fato_desempenho f
JOIN ouro.Dim_Comportamento c ON f.Id_Comportamento = c.Codigo
WHERE f.Categ_Nota = 'HIGH'
GROUP BY c.Motivacao
ORDER BY qtd DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Qual a media numerico para variaveis numericas entre alunos de notas altas?

-- COMMAND ----------

SELECT
  AVG(est.Horas_Estudadas) AS media_h_estudadas,
  AVG(est.Tutorias) AS media_tutorias,
  AVG(est.Frequencia) AS media_frequencia
FROM ouro.Dim_Estudo est
JOIN ouro.fato_desempenho f ON f.Id_Estudo = est.Codigo
WHERE f.Categ_Nota = 'HIGH';
