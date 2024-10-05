# Analise das Empresas do Brasil

## Objetivo do Projeto

### Este projeto de engenharia de dados tem como foco fornecer um panorama abrangente sobre a situação das empresas no Brasil. Nosso objetivo é identificar o número de empresas em operação, mapear as regiões com maior concentração de empresas e destacar suas principais atividades econômicas. Para isso, foi desenvolvido um pipeline de dados completo, utilizando diversas ferramentas da AWS e tendo como base os dados disponibilizados pelo governo brasileiro por meio do [Portal de dados](https://dados.gov.br/dados/conjuntos-dadoscadastro-nacional-da-pessoa-juridica---cnpj) 

## Principais Componentes:

![Arquitetura](link)

 - **Fonte de dados:** arquivos CSV provenientes no [Portal de dados](https://dados.gov.br/dados/conjuntos-dadoscadastro-nacional-da-pessoa-juridica---cnpj)


- **Ingestão:** Utilização do Apache Spark para ingestão e carregamento dos arquivos CSV no bucket AWS S3.

- **Armazenamento:** Estruturação dos dados em três camadas - Bronze, Silver e Gold, permitindo o enriquecimento e a transformação gradual das informações.

- **Processamento:** A execução dos processos de transformação entre as zonas de armazenamento foi realizada com o AWS EMR, proporcionando escalabilidade e eficiência no processamento de grandes volumes de dados.

- **Visualização:** O Power BI foi utilizado para criar dashboards que apresentam os insights extraídos dos dados.

## Conclusão

Este projeto proporcionou uma visão ampla e detalhada da distribuição e das atividades econômicas das empresas no Brasil. A implementação de um pipeline de dados estruturado permitiu a ingestão, armazenamento e processamento eficaz de grandes volumes de dados, garantindo que informações valiosas fossem extraídas e apresentadas de maneira clara e acessível.

O uso de ferramentas da AWS, como o EMR e o S3, juntamente com a arquitetura em camadas (Bronze, Silver e Gold), possibilitou o processamento escalável e otimizado dos dados. A apresentação no Power BI facilitou a análise visual e a compreensão dos principais insights, como a concentração de empresas por região e os setores econômicos dominantes.

Este pipeline pode ser facilmente adaptado e escalado para diferentes tipos de análises ou para incorporar novos dados à medida que surgirem.