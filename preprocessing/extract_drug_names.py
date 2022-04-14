from pyspark.sql import SparkSession


if __name__ == '__main__':
    # PART ONE
    file = '/home/y4tsu/Documents/uci_ml/full_raw.csv'
    spark = SparkSession.builder.master("local[*]").appName("drug_name_finder").getOrCreate()
    df = spark.read.load(file, header='true', format='csv', escape='"', multiLine='true', mode='FAILFAST')
    # print(df.count())  # = 164_788
    drug = df.select(df['drugName'])
    drug = drug.distinct()
    # print(drug.count())  # = 3_454
    drug.show(5)

    drug.write.text('drugs')
    exit()

    # PART TWO: in bash: cat *.txt > full-list.txt

    # PART THREE
    allow_list = ['af', 'pen', 'free', 'e', 'hair', 'weekly', 'less', 'rice', 'mixture', 'sinus', 'dose', 'extra', 'mini',
                  'foot', 'commit', 'mouthwash', 'gold', 'cosmetic', 'todays', 'choice', 'body', 'r', 'ms', 'bp',
                  'concept', 'nighttime', '&', 'kit', 'glucose', 'heather', 'foaming', 'cultivate', 'or', 'time', 'freeze',
                  'athlete\'s', 'oasis', 'bc', 'one-step', 'skin', 'filler', 'sleep', 'la', 'bag', 'factor', 'extended',
                  'lax', 'diabetic', 'jelly', 'ultra', 'water', 'burn', 'liquid', 'natal', 'cleanser', 'pressure', 'nail',
                  'a-d', 'slippery', 'dermal', 'fever', 'severe', 'tar', 'simply', 'hp', '+', 'carbohydrate', 'sparkling',
                  'saline', 'assure', 'health', 'rid', 'light', 'long-acting', 'long', 'acting', 'suppository', 'suppositories',
                  'multi-symptom', 'smoothie', 'aid', 'at', 'iv', 'rapid', 'c', 'b', 'a', 'ocular', 'alcohol', 'minerals',
                  'mix', 'fast', 'mist', 'desiccated', 'pain', 'day', 'impulse', 'royal', 'combination', 'way', 'paste',
                  'tincture', 'balance', 'and', 'er', 'lip', 'take', 'horse', 'sprinkles', 'd', 'night', 'cd', 'group',
                  'gas', 'aqua', 'smoothies', 'refresh', 'balsam', 'hd', 'drying', 'my', '(d)', 'met', 'pollen', 'cracked',
                  'p', 'hbp', 'ib', 'duo', 'lp', 'extreme', 'ac', 'patch', 'women', 'versed', 'wash', 'headache', 'all',
                  'system', 'micro', 'drowsy', 'gel', 'cream', 'plan', 'chest', 'butt', 'pads', 'tums', 'move', 'human',
                  'women\'s', 'oil', 'solo', 'sd', 'flu', 'bowel', 'original', 'ortho', 'violet', 'plus', 'force', 'allergy',
                  'pills', 'totality', 'throat', 'shampoo', 'strength', 'n', 'saliva', 'depot', 'cell', 'coagulation',
                  'tears', 'relief', 'cold', 'ointment', 'urinary', 'milk', 'triple', 'hour', 'raspberry', 'topical',
                  'vanquish', 'fleet', 'spray', 's', 'g', 'solution', 'sore', 'adult', 'enema', 'scalp', 'pack', 'sensitive',
                  'dual', 'bee', 'v', 'timespan', 'im', 'pr', 'formulation)', 'salt', 'regrowth', 'colon', 'tablets',
                  'lubricant', 'mumps', 'type', 'pro', 'otc', 'fatty', 'regular', 'solutions', 'vaginal', 'aerosol',
                  'blue', 'red', 'slow', 'vitamin', 'of', 'f', 'back', 'hormone', 'lotion', 'first', 'nasal', 'action',
                  'prep', 'immune', 'for', 'yeast', 'moisturizing', 'treatment', 'softener', 'pe', 'substitutes', 'children\'s',
                  'es', 'cranberry', 'oral', 'leader', 'tiger', 'hemorrhoids', 'cough', 'influenza', 'wafer', 'one', 'full',
                  'coal', 'conjugated', 'follicle', 'tape', 'next', 'sl', 'virus', 'acting', 'saw', 'stool', 'chewable',
                  'peru', 'yellow', 'men\'s', 'soda', 'lo', 'pm', 'ar', 'acne', 'e', 'laxative', 'armour', 'balm', 'chain',
                  'compound', 'contraceptive', 'corn', 'cultivate', 'decongestant', 'dm', 'emollients', 'ez', 'extra-strength',
                  'fml', 'ii', 'inactivated', 'inhaler', 'injection', 'lactate', 'live', 'maximum', 'medicine', 'medicated',
                  'medium', 'migraine', 'nails', 'np', 'nutrition', 'osteo', 'pinworm', 'pneumococcal', 'powder', 'powders',
                  'prenatal', 'pumpspray', 'rabies', 'replacement', 'sequels', 'silver', 'stimulating', 'subcutaneous',
                  'tandem', 'tension', 'thyroid', 'vaccine', 'viscous', 'with']

    # Post-processing to make the list compatible with the final output
    drugs_set = set()
    with open('all.txt', 'r') as r:
        for drug in r.readlines():
            drug = drug.strip('\n')
            # Words are split by space in the preprocessor so any drug name containing the spaces should also be divided up
            drug_split = []
            temp = drug.split(' / ')
            for x in temp:
                drug_split += x.split()
            for part in drug_split:
                # print(part)
                if any([x in '0123456789.' for x in part]):
                    continue
                part = part.strip(',')
                part = part.lower()
                if part not in allow_list:
                    drugs_set.add(part)

    drugs_list = list(drugs_set)
    print(f'Total length of drugs set: {len(drugs_set)}')

    with open('drugs_list.txt', 'a') as a:
        for drug in drugs_list:
            a.write(f'{drug}\n')
