import (
	"context"
	"github.com/chromedp/chromedp"
)

func main() {
	ctx, cancel := chromedp.NewContext(context.Background())
	defer cancel()

	var networkEvents []string

	chromedp.ListenTarget(ctx, func(ev interface{}) {
		switch e := ev.(type) {
		case *network.EventRequestWillBeSent:
			networkEvents = append(networkEvents, e.Request.URL)
		}
	})

	chromedp.Run(ctx,
		chromedp.Navigate("https://example.com"),
	)

	for _, url := range networkEvents {
		fmt.Println("Fetch/XHR:", url)
	}
}