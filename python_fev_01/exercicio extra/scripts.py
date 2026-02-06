import random # para fazer o sorteio

numeros_usuario = []
sorteados = []

while len(numeros_usuario) <6: # len() conta quantos numeros entrou na lista
    try:
        num = int(input(f'Digite um número entre 1 e 100: '))

        if num <1 or num> 100: #recusa nº fora do intervalo
            print('O número está fora do intervalo válido, tente novamente')
        
        elif num in numeros_usuario: #recusa números repetidos
            print('Número repetido! Digite um novo número ente 1 e 100.')
            
        elif len(numeros_usuario) == 6:
            print(f'Os números digitados foram {numeros_usuario}, aguarde o sorteio.')

        elif len(numeros_usuario) >6:
            print(f'Você digitou números suficientes, aguarde o sorteio!')  
        else:
            numeros_usuario.append(num) # .append() adiciona o número digitado na lista 
           

    except ValueError: #recusa se não for número
        print('Esta opção não é válida! Digite apenas números entre 1 e 100.')

print(f'Os números digitados foram: {numeros_usuario}.')

if len(numeros_usuario) == 6:
    sorteados = random.sample(range(1,101),6) #cria a lista de uma vez e retorna pa a variável, sem nº repetido
    print(f'Os números sorteados foram: {sorteados}.')

acertos = [ n for n in numeros_usuario if n in sorteados]  
print(f'Você acertou {len(acertos)} números. Os numeros foram {acertos}.')

if len(acertos) == 6:
    print(f'Parabéns, você acertou todos os números sorteados! Os números são {acertos}.')

else: 
    print(f'Não foi desta vez.Tente novamente!')
