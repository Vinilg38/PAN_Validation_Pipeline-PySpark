import csv
import random
import string
import os


def generate_valid_pan():
    """Generates a valid PAN number format: [A-Z]{5}[0-9]{4}[A-Z]"""
    # 5 random uppercase letters
    letters = ''.join(random.choices(string.ascii_uppercase, k=5))
    # 4 random digits
    digits = ''.join(random.choices(string.digits, k=4))
    # 1 random uppercase letter
    last_letter = random.choice(string.ascii_uppercase)
    return letters + digits + last_letter


def generate_invalid_pan_with_symbol():
    """Generates a PAN with a symbol in a random position."""
    pan = generate_valid_pan()
    symbol = random.choice(['@', '#', '$', '%', '&', '*'])
    pos = random.randint(0, 9)
    return pan[:pos] + symbol + pan[pos + 1:]


def generate_invalid_pan_with_mixed_case():
    """Generates a PAN with a mixture of uppercase and lowercase letters."""
    pan = generate_valid_pan()
    pos = random.randint(0, 9)
    return pan.lower()


def generate_invalid_pan_with_sequence():
    """Generates a PAN with a sequential sequence (e.g., ABCD)."""
    pan = list(generate_valid_pan())
    # Create a 4-char sequence and a 5-char sequence
    seq_start_char = random.choice(string.ascii_uppercase)
    seq = ''.join(chr(ord(seq_start_char) + i) for i in range(4))
    if random.choice([True, False]):
        # Place the sequence in the first 5 letters
        pan[1:5] = list(seq)
    else:
        # Place the sequence in the 4 digits
        seq_start_digit = str(random.randint(0, 6))
        seq_digits = ''.join(str(int(seq_start_digit) + i) for i in range(4))
        pan[5:9] = list(seq_digits)
    return "".join(pan)


def generate_invalid_pan_with_adjacent():
    """Generates a PAN with adjacent identical characters."""
    pan = list(generate_valid_pan())
    repeat_char = random.choice(string.ascii_uppercase)
    pos = random.randint(0, 8)
    pan[pos] = repeat_char
    pan[pos + 1] = repeat_char
    return "".join(pan)


def generate_invalid_pan_incorrect_length():
    """Generates a PAN with an incorrect length (9 or 11 characters)."""
    if random.choice([True, False]):
        return ''.join(random.choices(string.ascii_uppercase + string.digits, k=9))
    else:
        return ''.join(random.choices(string.ascii_uppercase + string.digits, k=11))


def generate_null_or_empty_pan():
    """Generates a null or empty PAN."""
    return random.choice([None, '', '   '])


# --- Main Script ---
file_path = "/Users/vinilg7/PycharmProjects/PAN_Validation/PAN Number Validation Dataset Billion.csv"
num_records_to_append = 999000000

print(f"Generating and appending {num_records_to_append} records to {file_path}...")

# Check if file exists, if not, create it with a header
if not os.path.exists(file_path):
    with open(file_path, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['pan_no'])

with open(file_path, 'a', newline='') as f:
    writer = csv.writer(f)
    for i in range(num_records_to_append):
        if i % 10 == 0:  # 10% of records are invalid symbols
            record = generate_invalid_pan_with_symbol()
        elif i % 10 == 1:  # 10% are incorrect length
            record = generate_invalid_pan_incorrect_length()
        elif i % 10 == 2:  # 10% are null or empty
            record = generate_null_or_empty_pan()
        elif i % 10 == 3:  # 10% are mixed case
            record = generate_invalid_pan_with_mixed_case()
        elif i % 10 == 4:  # 10% have sequential characters
            record = generate_invalid_pan_with_sequence()
        elif i % 10 == 5:  # 10% have adjacent characters
            record = generate_invalid_pan_with_adjacent()
        else:  # 40% are valid
            record = generate_valid_pan()

        writer.writerow([record])

print("Generation complete! The file has been updated.")