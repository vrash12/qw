import binascii

def inject_amount(static_payload: str, amount: str) -> str:
    # strip old CRC (last 8 chars)
    base = static_payload[:-8]

    # find end of field 53
    idx53 = base.find('53')
    len53 = int(base[idx53+2:idx53+4])
    end53 = idx53 + 4 + len53

    # build amount field 54
    amt_field = f"54{len(amount):02d}{amount}"

    # splice in and compute new CRC
    new_base = base[:end53] + amt_field + base[end53:]
    crc = binascii.crc_hqx((new_base + "6304").encode(), 0) & 0xFFFF
    return new_base + "6304" + f"{crc:04X}"


#call now this


