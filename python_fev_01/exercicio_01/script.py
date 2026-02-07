import pandas as pd 

vendas_df = pd.read_csv('vendas.csv')

vendas_limpo_df = vendas_df.dropna() # exclui todas as LINHAS que possuem ao menos um NaN

vendas_por_produto = vendas_limpo_df.groupby('produto')['quantidade'].sum()
#print(vendas_por_produto)

vendas_limpo_df['faturamento']= vendas_limpo_df['quantidade'] * vendas_limpo_df['preco'] # cria a coluna faturamento
faturamento_por_categoria = vendas_limpo_df.groupby('categoria')['faturamento'].sum() #agrupa por categoria e soma o total
faturamento_por_categoria = faturamento_por_categoria.sort_values(ascending = False)
#print(faturamento_por_categoria)

ranking_vendedores = vendas_limpo_df.groupby('vendedor')['faturamento'].sum()# agrupa por vendedor e soma o faturamento
ranking_vendedores+ranking_vendedores.sort_values(ascending=False) # ordem que será exibido
#print(ranking_vendedores)

faturamento_por_cidade = vendas_limpo_df.groupby('cidade')['faturamento'].sum()# agrupamento por cidade e faturamento
#print(faturamento_por_cidade)

faturamento_por_data = vendas_limpo_df.groupby('data')['faturamento'].sum()
#print(faturamento_por_data)

forma_pagamento = vendas_limpo_df.groupby('forma_pagamento').size() # .size() determina a frequencia
forma_pagamento= forma_pagamento.sort_values(ascending=False) #ordena a partir da forma mais usada
#print(forma_pagamento)

linhas_originais=len(vendas_df)
linhas_finais = len(vendas_limpo_df)
linhas_removidas = linhas_originais - linhas_finais
#print(linhas_finais)

contagem_vazios = vendas_df.isnull().sum() # soma quantos vazios tem na tabela original
ranking_vazios = contagem_vazios.sort_values(ascending = False) #ordena de forma decrescente
#print(ranking_vazios)

# Salvando o DataFrame limpo em um novo arquivo CSV(padrão internacional para o GitHub)
vendas_limpo_df.to_csv('vendas_limpas_final.csv', index=False, encoding='utf-8', sep=',')

