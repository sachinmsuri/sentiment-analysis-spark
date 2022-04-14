import pyspark.sql.functions as F
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.mllib.evaluation import MulticlassMetrics


class Metrics:
    """ Utility functions for abstracting metrics calculations and displays. """
    def __init__(self):
        self._acc_evaluator = MulticlassClassificationEvaluator(metricName='accuracy')
        self._f1_evaluator = MulticlassClassificationEvaluator(metricName='f1')
        self.mx = None
        self.key = {0: 'negative', 1: 'neutral', 2: 'positive'}

    def accuracy(self, pl):
        """ Print the overall accuracy of predictions in the predictions & labels DataFrame. """
        print(f'Accuracy on given DataFrame: {self._acc_evaluator.evaluate(pl) * 100:.1f}%\n')

    def f1(self, pl):
        """ Print the overall F1 score of predictions in the predictions & labels DataFrame. """
        print(f'F1 score for given DataFrame: {self._f1_evaluator.evaluate(pl) * 100:.1f}\n')

    def _pl_to_mx_rdd(self, pl):
        """ Map the preds & labels DataFrame to an RDD in correct format for MulticlassMetrics API. """
        temp = pl.select(['prediction', 'label']).withColumn('label', F.col('label').cast('float')).orderBy('prediction')
        self.mx = MulticlassMetrics(temp.rdd.map(tuple))

    def confusion_matrix(self, pl):
        """ Print a confusion matrix of the predictions and labels. """
        if not self.mx:
            self._pl_to_mx_rdd(pl)

        # Generate the confusion matrix
        confusion_matrix = self.mx.confusionMatrix().toArray().tolist()

        # Print the confusion matrix nicely
        print('                Predictions')
        print(f'            {self.key[0]:<12}{self.key[1]:<12}{self.key[2]:<12}')
        for i, row in enumerate(confusion_matrix):
            print(f'{self.key[i]:<12}{int(row[0]):<12}{int(row[1]):<12}{int(row[2]):<12}')
        print('(True labels are on the side)\n')

    def performance(self, pl):
        """ Print precision, accuracy and F1 scores for all classes. """
        if not self.mx:
            self._pl_to_mx_rdd(pl)

        print('Class     | Precision | Recall    | F1-score')
        for k in self.key:
            print(f'{self.key[k]:<12}{self.mx.precision(float(k)) * 100:<12.1f}{self.mx.recall(float(k)) * 100:<12.1f}{self.mx.fMeasure(float(k), beta=1.) * 100:<12.1f}')
        print('')
