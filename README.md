# 🧠 Análise de Performance de Estudantes – MVP Dados

Projeto desenvolvido como estudo de modelagem, análise e visualização de dados educacionais. O objetivo principal é identificar os fatores que mais impactam o desempenho acadêmico dos estudantes com base em variáveis comportamentais, socioeconômicas e escolares.

## 📝 Documentação e informações do projeto disponíveis no link abaixo 
[Documentação Análise de Performance de Estudantes](https://www.notion.so/An-lise-de-Performance-de-Estudantes-MVP-Dados-1bcee8e561e580cf9336c682550a079c?pvs=4)

---

## 💻 Notebook disponível no link para o DataBricks
[Notebook Performance de Alunos](https://databricks-prod-cloudfront.cloud.databricks.com/public/4027ec902e239c93eaaa8714f173bcfc/1637540305527518/1228332239544535/145150705971040/latest.html)
---

## 📊 Objetivo

Investigar quais características (individuais e contextuais) estão associadas ao alto desempenho estudantil (nota ≥ 85), utilizando modelagem dimensional, análises estatísticas e visualizações interativas.

---

## 🧱 Arquitetura do Projeto

Estruturado com base em um modelo Lakehouse e pipeline de dados dividido em 3 camadas:

- **Bronze**: ingestão de dados brutos
- **Prata**: tratamento, limpeza e modelagem dimensional (modelo estrela)
- **Ouro**: dados analíticos otimizados para visualização e exploração

---

## 🗃️ Modelagem Dimensional

Modelo estrela com as seguintes tabelas:

**Fato:**
- `fato_desempenho`: contém as notas dos alunos e os IDs de ligação com dimensões

**Dimensões:**
- `dim_aluno` (Gênero, Situação Financeira, Suporte dos Pais etc.)
- `dim_escola` (Tipo de Escola, Notas Anteriores, Distância etc.)
- `dim_estudo` (Horas Estudadas, Frequência, Tutorias etc.)
- `dim_comportamento` (Motivação, Atividades, Sono, Influência etc.)

---

## 📈 Principais Análises e Descobertas

### 🔹 Alunos de Alta Performance (Nota ≥ 85)

- **Alta frequência** escolar (média de **81%**) e **tempo de estudo** (média de **20h/semana**) foram os fatores mais correlacionados com o bom desempenho (correlações de 0.58 e 0.45, respectivamente).
- **85% dos melhores alunos não possuem dificuldades de aprendizado.**
- **82%** desses alunos tiveram **influência positiva ou neutra**.
- A **motivação** não apresentou impacto isolado significativo.

### 🔹 Variáveis Categóricas

- **Tipo de escola**: 71% dos alunos com alto desempenho vieram de escolas públicas.
- **Gênero**: distribuição equilibrada (53% homens, 47% mulheres).
- **Situação financeira**: alunos de baixa, média e alta renda estão relativamente equilibrados no grupo de alta performance.

### 🔹 Variáveis Numéricas

| Variável             | Valor Médio (Alunos HIGH) | Observações |
|----------------------|---------------------------|-------------|
| Horas Estudadas      | 20h/semana                | Alta correlação com nota |
| Frequência Escolar   | 81%                       | Maior fator de impacto |
| Tutorias             | 1.4 sessões               | Baixo impacto |
| Horas de Sono        | 7h por dia                | Importante para equilíbrio, mas neutra na correlação |
| Notas Anteriores     | 75                        | Baixo impacto |

---

## 📊 Visualizações

Utilizando SQL e Python (Pandas, Seaborn e Matplotlib) foram geradas análises como:

- Boxplots de variáveis numéricas por grupo de nota
- Gráficos de barras empilhadas (proporções por categoria)
- Heatmaps de correlação

---

## 🧪 Ferramentas Utilizadas

- Databricks (SQL + Notebooks)
- PySpark e Spark SQL
- Pandas, Seaborn e Matplotlib
- Delta Lake
- Modelagem Dimensional (Estrela)
- Camadas de dados: Bronze / Prata / Ouro

---

## 🧠 Conclusão

Para alcançar boas notas, os dados mostram que o aluno precisa manter **alta frequência nas aulas**, dedicar **tempo de estudo semanal** e ter **ambiente social favorável** (influência positiva e suporte dos pais). Fatores como motivação ou renda, embora relevantes, não se mostraram determinantes de forma isolada.

---

## 📌 Autor

Projeto desenvolvido por Amanda Amaral.  
Baseado em dados educacionais internacionais para fins de estudo e desenvolvimento de MVP em Análise de Dados.

---
