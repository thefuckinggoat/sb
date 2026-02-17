package utils

import (
	"fmt"
	"image"
	"image/color"
	"image/draw"
	"image/png"
	"log"
	"os"
	"runtime"
	"strings"

	"golang.org/x/image/font"
	"golang.org/x/image/font/basicfont"
	"golang.org/x/image/font/opentype"
	"golang.org/x/image/math/fixed"
)

var (
	titleFace   font.Face
	contentFace font.Face
	infoFace    font.Face
)

func init() {
	fontPath := getSystemFont()
	if fontPath != "" {
		if err := loadFonts(fontPath); err != nil {
			log.Printf("[image_utils] Error loading fonts: %v, using default", err)
			setDefaultFonts()
		}
	} else {
		log.Println("[image_utils] No system font found, using default font")
		setDefaultFonts()
	}
}

func getSystemFont() string {
	system := runtime.GOOS
	var paths []string

	switch system {
	case "windows":
		paths = []string{
			"C:/Windows/Fonts/Arial.ttf",
			"C:/Windows/Fonts/consola.ttf",
			"C:/Windows/Fonts/segoeui.ttf",
		}
	case "darwin":
		paths = []string{
			"/Library/Fonts/Arial.ttf",
			"/System/Library/Fonts/Helvetica.ttc",
		}
	default: // linux
		paths = []string{
			"/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
			"/usr/share/fonts/TTF/Arial.ttf",
			"/usr/share/fonts/truetype/freefont/FreeSans.ttf",
		}
	}

	for _, p := range paths {
		if _, err := os.Stat(p); err == nil {
			return p
		}
	}
	return ""
}

func loadFonts(fontPath string) error {
	data, err := os.ReadFile(fontPath)
	if err != nil {
		return err
	}

	f, err := opentype.Parse(data)
	if err != nil {
		return err
	}

	titleFace, err = opentype.NewFace(f, &opentype.FaceOptions{Size: 16, DPI: 72})
	if err != nil {
		return err
	}
	contentFace, err = opentype.NewFace(f, &opentype.FaceOptions{Size: 14, DPI: 72})
	if err != nil {
		return err
	}
	infoFace, err = opentype.NewFace(f, &opentype.FaceOptions{Size: 12, DPI: 72})
	if err != nil {
		return err
	}
	return nil
}

func setDefaultFonts() {
	titleFace = basicfont.Face7x13
	contentFace = basicfont.Face7x13
	infoFace = basicfont.Face7x13
}

// GenerateCardImage generates a card image from results
func GenerateCardImage(results []string, totalResults int, page int, totalPages int) (string, error) {
	maxResults := 10
	if len(results) > maxResults {
		results = results[:maxResults]
	}

	padding := 20
	borderWidth := 2
	textColor := color.RGBA{255, 255, 255, 255}
	bgColor := color.RGBA{0, 0, 0, 255}
	lineSpacing := 4
	resultSpacing := 15

	numColumns := len(results)/2 + 1
	if numColumns > 3 {
		numColumns = 3
	}
	if numColumns < 1 {
		numColumns = 1
	}

	maxWidth := 400 * numColumns
	columnWidth := (maxWidth - padding*(numColumns+1)) / numColumns

	// Calculate height
	totalHeight := padding * 2
	lineHeight := 16

	for _, result := range results {
		lines := strings.Split(result, "\n")
		for _, line := range lines {
			wrapped := wrapText(line, columnWidth, 7)
			totalHeight += len(wrapped) * (lineHeight + lineSpacing)
		}
		totalHeight += resultSpacing
	}
	totalHeight += 30 // info text

	if totalHeight < 100 {
		totalHeight = 100
	}

	imgWidth := maxWidth
	imgHeight := totalHeight + borderWidth*2

	img := image.NewRGBA(image.Rect(0, 0, imgWidth, imgHeight))

	// Fill background
	draw.Draw(img, img.Bounds(), &image.Uniform{bgColor}, image.Point{}, draw.Src)

	// Draw border
	borderColor := color.RGBA{255, 255, 255, 255}
	for x := 0; x < imgWidth; x++ {
		for y := 0; y < borderWidth; y++ {
			img.Set(x, y, borderColor)
			img.Set(x, imgHeight-1-y, borderColor)
		}
	}
	for y := 0; y < imgHeight; y++ {
		for x := 0; x < borderWidth; x++ {
			img.Set(x, y, borderColor)
			img.Set(imgWidth-1-x, y, borderColor)
		}
	}

	// Draw text
	yOffset := padding + borderWidth
	xOffset := padding + borderWidth

	for _, result := range results {
		lines := strings.Split(result, "\n")
		for i, line := range lines {
			face := contentFace
			if i == 0 {
				face = titleFace
			}
			wrapped := wrapText(line, columnWidth, 7)
			for _, wl := range wrapped {
				drawText(img, xOffset, yOffset+lineHeight, wl, face, textColor)
				yOffset += lineHeight + lineSpacing
			}
		}
		yOffset += resultSpacing
	}

	// Draw info text
	infoText := fmt.Sprintf("Showing %d out of %d results - Page %d/%d", len(results), totalResults, page, totalPages)
	drawText(img, padding, imgHeight-padding, infoText, infoFace, textColor)

	// Save to temp file
	tmpFile := fmt.Sprintf("/tmp/card_%d_%d.png", page, totalPages)
	f, err := os.Create(tmpFile)
	if err != nil {
		return "", err
	}
	defer f.Close()

	if err := png.Encode(f, img); err != nil {
		return "", err
	}

	return tmpFile, nil
}

func drawText(img *image.RGBA, x, y int, text string, face font.Face, col color.Color) {
	d := &font.Drawer{
		Dst:  img,
		Src:  image.NewUniform(col),
		Face: face,
		Dot:  fixed.Point26_6{X: fixed.I(x), Y: fixed.I(y)},
	}
	d.DrawString(text)
}

func wrapText(text string, maxWidth int, charWidth int) []string {
	if text == "" {
		return nil
	}

	maxChars := maxWidth / charWidth
	if maxChars <= 0 {
		maxChars = 1
	}

	words := strings.Fields(text)
	if len(words) == 0 {
		return nil
	}

	var lines []string
	currentLine := ""

	for _, word := range words {
		test := currentLine
		if test != "" {
			test += " "
		}
		test += word

		if len(test) > maxChars && currentLine != "" {
			lines = append(lines, strings.TrimSpace(currentLine))
			currentLine = word
		} else {
			currentLine = test
		}
	}

	if strings.TrimSpace(currentLine) != "" {
		lines = append(lines, strings.TrimSpace(currentLine))
	}

	return lines
}