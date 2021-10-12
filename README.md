## Desafio Final Bootcamp - IGTI

### Engenheiro de Dados Cloud
#### Professores: Neylson Crepalde, Carlos Barbosa e Pedro Toledo

# Use Case:

Pipeline desenvolvido no kuberentes para a extração dos dados diretamento do site do Governo Federal, ingestão na camada landing-zone do data lake, processamento e transformação com spark e disponibilização para consulta no Amazon Athena dos microdados do Censo da Educação Superior 2019.
Todo o processo de ELT foi orquestrado pelo Apache Airflow.

Arquitetura da solução: 

![Kubernetes,EKS](img/k8sgitairflow.png)

## Dags Airflow

![airflow](img/dags.png)

