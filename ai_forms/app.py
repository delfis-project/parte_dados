import pandas as pd
import joblib
from flask import Flask, render_template, request
import os

ROOT_DIR = os.path.dirname(__file__)

app = Flask(__name__)

model_path = os.path.join(ROOT_DIR, 'pipeline.pkl')
pipeline_carregado = joblib.load(model_path)

@app.route('/', methods=['GET', 'POST'])
def home():
    if request.method == 'POST':
        faixa_etaria = request.form['faixa_etaria']
        estado = request.form['estado']
        renda = request.form['renda']
        familiaridade = request.form['familiaridade']
        tempo_celular = request.form['tempo_celular']
        interesse_jogos = request.form['interesse_jogos']

        input_features = pd.DataFrame([{
            'Qual a sua faixa etária?': faixa_etaria,
            'Em qual estado você mora?': estado,
            'Qual sua faixa de renda familiar?': renda,
            'Qual é o seu nível de familiaridade com o uso de aplicativos em dispositivos móveis?': familiaridade,
            'Em média, quanto tempo você usa o celular diariamente?': tempo_celular,
            'Você se interessaria em jogar palavra cruzada, sudoku ou jogos similares a esses diariamente?': interesse_jogos
        }])

        prediction = pipeline_carregado.predict(input_features)
        return render_template("prediction.html", prediction=prediction[0])

    return render_template("index.html")

if __name__ == '__main__':
    app.run(host="0.0.0.0", debug=True)
