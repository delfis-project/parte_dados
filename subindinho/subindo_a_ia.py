from flask import Flask, render_template, request
import os
import joblib  # Importar joblib para carregar o pipeline
import pandas as pd  # Importar pandas para manipulação de dados

# Define the root directory
ROOT_DIR = os.path.dirname(__file__)

# Initialize the Flask application
app = Flask(__name__)

# Load the pipeline (e.g., machine learning model and preprocessor)
model_path = os.path.join(ROOT_DIR, 'pipeline.pkl')  # Path to your pickle file
pipeline_carregado = joblib.load(model_path)  # Carregando o pipeline diretamente

@app.route('/', methods=['GET', 'POST'])
def home():
    if request.method == 'POST':
        # Get data from the form
        faixa_etaria = request.form['faixa_etaria']
        estado = request.form['estado']
        renda = request.form['renda']
        familiaridade = request.form['familiaridade']
        tempo_celular = request.form['tempo_celular']
        interesse_jogos = request.form['interesse_jogos']

        # Prepare input data for the model as a DataFrame
        input_features = pd.DataFrame([{
            'Qual a sua faixa etária?': faixa_etaria,
            'Em qual estado você mora?': estado,
            'Qual sua faixa de renda familiar?': renda,
            'Qual é o seu nível de familiaridade com o uso de aplicativos em dispositivos móveis?': familiaridade,
            'Em média, quanto tempo você usa o celular diariamente?': tempo_celular,
            'Você se interessaria em jogar palavra cruzada, sudoku ou jogos similares a esses diariamente?': interesse_jogos
        }])

        # Transform input data using the pipeline (this will handle both preprocessing and prediction)
        prediction = pipeline_carregado.predict(input_features)  # Passar o DataFrame para o predict

        # Render prediction result in a new template
        return render_template("prediction.html", prediction=prediction[0])

    return render_template("index.html")

if __name__ == '__main__':
    app.run(debug=True)
