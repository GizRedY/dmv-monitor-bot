"""
Simple icon generator for PWA
Creates basic colored icons with text
"""

try:
    from PIL import Image, ImageDraw, ImageFont
except ImportError:
    print("ERROR: Pillow not installed")
    print("Install it with: pip install Pillow")
    exit(1)

def create_icon(size, filename):
    """Create a simple colored icon"""
    # Create image with gradient background
    img = Image.new('RGB', (size, size), color='white')
    draw = ImageDraw.Draw(img)
    
    # Draw gradient background (blue-ish)
    for y in range(size):
        # Gradient from #4F46E5 to #764ba2
        r = int(79 + (118 - 79) * y / size)
        g = int(70 + (75 - 70) * y / size)
        b = int(229 + (162 - 229) * y / size)
        draw.line([(0, y), (size, y)], fill=(r, g, b))
    
    # Draw white circle in center
    circle_size = int(size * 0.7)
    circle_pos = (size - circle_size) // 2
    draw.ellipse(
        [circle_pos, circle_pos, circle_pos + circle_size, circle_pos + circle_size],
        fill='white',
        outline=None
    )
    
    # Draw text "DMV"
    font_size = int(size * 0.2)
    try:
        # Try to use a system font
        try:
            font = ImageFont.truetype("arial.ttf", font_size)
        except:
            try:
                font = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf", font_size)
            except:
                # Fallback to default
                font = ImageFont.load_default()
    except:
        font = ImageFont.load_default()
    
    text = "DMV"
    
    # Get text bounding box
    bbox = draw.textbbox((0, 0), text, font=font)
    text_width = bbox[2] - bbox[0]
    text_height = bbox[3] - bbox[1]
    
    # Center text
    text_x = (size - text_width) // 2
    text_y = (size - text_height) // 2 - bbox[1]
    
    draw.text((text_x, text_y), text, fill='#4F46E5', font=font)
    
    # Save
    img.save(filename, 'PNG')
    print(f"✅ Created {filename} ({size}x{size})")

def main():
    print("=" * 50)
    print("PWA Icon Generator")
    print("=" * 50)
    print()
    
    # Create both sizes
    create_icon(192, 'icon-192.png')
    create_icon(512, 'icon-512.png')
    
    print()
    print("=" * 50)
    print("Icons created successfully!")
    print("=" * 50)
    print()
    print("Note: These are basic placeholder icons.")
    print("For production, consider creating custom icons:")
    print("  - Use a design tool like Figma or Canva")
    print("  - Or use an online icon generator")
    print("  - Recommended: https://favicon.io/")
    print()

if __name__ == "__main__":
    main()
