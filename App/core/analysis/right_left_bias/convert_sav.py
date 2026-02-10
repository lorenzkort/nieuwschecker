import pyreadstat


def convert_sav_to_csv(input_file):
    try:
        df, meta = pyreadstat.read_sav(input_file)
        output_file = input_file.replace(".sav", ".csv")
        df.to_csv(output_file, index=False)
        print(f"Conversion successful. CSV file saved as '{output_file}'")
    except Exception as e:
        print(f"An error occurred: {e}")


if __name__ == "__main__":
    input_file = "/Users/lorenzkort/Downloads/Pew-Research-Center-Fall-2017-Media-and-Politics-in-Western-Europe-Survey/Pew Research Center Fall 2017 Media and Politics in Western Europe Survey.sav"
    convert_sav_to_csv(input_file)
