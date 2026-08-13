package explorer

import _ "embed"

var (
	//go:embed static/index.html
	indexHTML []byte

	//go:embed static/styles.css
	stylesCSS []byte

	//go:embed static/app.js
	appJS []byte
)
