from tensorflow import keras
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import Dense
import tensorflow as tf
import matplotlib.pyplot as plt
import numpy as np

# Creating the model with 2 hidden layers

model = Sequential()
model.add(Dense(30, activation='sigmoid', input_shape=(100,)))
model.add(Dense(15, activation='sigmoid'))
model.add(Dense(5, activation='softmax'))


# Training the model and saving its weights

model.compile(loss='categorical_crossentropy', metrics=['accuracy'])
model.build(input_shape=(None, 100))
model.save('dummy_model.keras')