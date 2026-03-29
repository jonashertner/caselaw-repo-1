#!/usr/bin/env python3
"""Generate branded OpenCaseLaw icons (16/32/64/128/300px).

Uses only Python stdlib (struct + zlib) to produce PNG files.
Red rounded square (#b91c24) with white 'O' ring for sizes >= 64.
"""

import math
import os
import struct
import zlib

SIZES = [16, 32, 64, 128, 300]
BG_COLOR = (0xB9, 0x1C, 0x24)  # #b91c24
WHITE = (255, 255, 255)
OUTPUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "assets")


def make_png(width, height, pixels):
    """Create PNG from RGBA pixel data (list of rows, each row = list of (r,g,b,a))."""
    def chunk(chunk_type, data):
        c = chunk_type + data
        crc = struct.pack(">I", zlib.crc32(c) & 0xFFFFFFFF)
        return struct.pack(">I", len(data)) + c + crc

    header = b"\x89PNG\r\n\x1a\n"
    ihdr = chunk(b"IHDR", struct.pack(">IIBBBBB", width, height, 8, 6, 0, 0, 0))

    raw = b""
    for row in pixels:
        raw += b"\x00"  # filter: none
        for r, g, b, a in row:
            raw += struct.pack("BBBB", r, g, b, a)

    idat = chunk(b"IDAT", zlib.compress(raw, 9))
    iend = chunk(b"IEND", b"")
    return header + ihdr + idat + iend


def rounded_rect_alpha(x, y, size, radius):
    """Return alpha (0-255) for pixel (x, y) in a rounded rectangle.
    Uses sub-pixel sampling (4x4) for anti-aliasing."""
    samples = 0
    for sy in range(4):
        for sx in range(4):
            px = x + (sx + 0.5) / 4.0
            py = y + (sy + 0.5) / 4.0
            # Check if inside rounded rect
            inside = True
            # Top-left corner
            if px < radius and py < radius:
                if (px - radius) ** 2 + (py - radius) ** 2 > radius ** 2:
                    inside = False
            # Top-right corner
            elif px > size - radius and py < radius:
                if (px - (size - radius)) ** 2 + (py - radius) ** 2 > radius ** 2:
                    inside = False
            # Bottom-left corner
            elif px < radius and py > size - radius:
                if (px - radius) ** 2 + (py - (size - radius)) ** 2 > radius ** 2:
                    inside = False
            # Bottom-right corner
            elif px > size - radius and py > size - radius:
                if (px - (size - radius)) ** 2 + (py - (size - radius)) ** 2 > radius ** 2:
                    inside = False
            if inside:
                samples += 1
    return int(samples * 255 / 16)


def ring_alpha(x, y, cx, cy, outer_r, inner_r):
    """Return alpha (0-255) for a ring (annulus) centered at (cx, cy).
    Uses 4x4 sub-pixel sampling for anti-aliasing."""
    samples = 0
    for sy in range(4):
        for sx in range(4):
            px = x + (sx + 0.5) / 4.0
            py = y + (sy + 0.5) / 4.0
            d = math.sqrt((px - cx) ** 2 + (py - cy) ** 2)
            if inner_r <= d <= outer_r:
                samples += 1
    return int(samples * 255 / 16)


def generate_icon(size):
    radius = size / 5.0
    pixels = []

    # Pre-compute ring params (only for size >= 64)
    draw_o = size >= 64
    cx = cy = size / 2.0
    outer_r = size * 0.35
    inner_r = size * 0.22

    for y in range(size):
        row = []
        for x in range(size):
            bg_a = rounded_rect_alpha(x, y, size, radius)
            if bg_a == 0:
                row.append((0, 0, 0, 0))
                continue

            if draw_o:
                o_a = ring_alpha(x, y, cx, cy, outer_r, inner_r)
                if o_a > 0:
                    # Blend white O over red background
                    t = o_a / 255.0
                    r = int(WHITE[0] * t + BG_COLOR[0] * (1 - t))
                    g = int(WHITE[1] * t + BG_COLOR[1] * (1 - t))
                    b = int(WHITE[2] * t + BG_COLOR[2] * (1 - t))
                    row.append((r, g, b, bg_a))
                else:
                    row.append((BG_COLOR[0], BG_COLOR[1], BG_COLOR[2], bg_a))
            else:
                row.append((BG_COLOR[0], BG_COLOR[1], BG_COLOR[2], bg_a))
        pixels.append(row)
    return make_png(size, size, pixels)


def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    for size in SIZES:
        path = os.path.join(OUTPUT_DIR, f"icon-{size}.png")
        data = generate_icon(size)
        with open(path, "wb") as f:
            f.write(data)
        print(f"Generated {path} ({len(data)} bytes)")


if __name__ == "__main__":
    main()
