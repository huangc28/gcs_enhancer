package gcsenhancer

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"image/gif"
	"image/jpeg"
	"image/png"
	"io"
	"log"
	"net/url"
	"path/filepath"
	"strings"
	"time"

	"image"

	"cloud.google.com/go/storage"
)

const GCSPublicHost = "storage.googleapis.com"

type GCSEnhancerInterface interface {
	ObjectLink(attr *storage.ObjectAttrs) string
	Upload(ctx context.Context, file io.Reader, uploadFilename string) (string, error)
}

type GCSEnhancer struct {
	client     *storage.Client
	bucketName string
}

func NewGCSEnhancer(client *storage.Client, bucketName string) *GCSEnhancer {
	return &GCSEnhancer{
		client:     client,
		bucketName: bucketName,
	}
}

func (e *GCSEnhancer) ObjectLink(attr *storage.ObjectAttrs) string {
	u := url.URL{
		Scheme: "https",
		Host:   GCSPublicHost,
		Path:   fmt.Sprintf("%s/%s", attr.Bucket, attr.Name),
	}

	return u.String()
}

func (e *GCSEnhancer) NewObjectWriter(ctx context.Context, filename string) *storage.Writer {
	bucket := e.client.Bucket(e.bucketName)
	object := bucket.Object(filename)

	return object.NewWriter(ctx)
}

func (e *GCSEnhancer) Upload(ctx context.Context, file io.Reader, uploadFilename string) (string, error) {
	bucket := e.client.Bucket(e.bucketName)
	object := bucket.Object(uploadFilename)
	objwriter := object.NewWriter(ctx)

	if _, err := io.Copy(objwriter, file); err != nil {
		return "", err
	}

	if err := objwriter.Close(); err != nil {
		return "", err
	}

	// ------------------- make the object publicly accessible -------------------
	if err := object.ACL().Set(ctx,
		storage.AllUsers,
		storage.RoleReader); err != nil {

		return "", err
	}

	// ------------------- retrieve object attributes -------------------
	attr, err := object.Attrs(ctx)

	if err != nil {
		return "", err
	}

	// ------------------- combine object link -------------------
	return e.ObjectLink(attr), nil
}

func AppendUnixTimeStampToFilename(filename string) string {
	secs := strings.Split(filename, ".")
	timeFactor := time.Now().Format("20060102150405")

	return fmt.Sprintf("%s_%s.%s", secs[0], timeFactor, secs[len(secs)-1])
}

func appendThumbnailStamp(filename string) string {
	secs := strings.Split(filename, ".")

	return fmt.Sprintf("%s_thumbnail.%s", secs[0], secs[len(secs)-1])
}

type Images struct {
	Name      string
	Mime      string
	OrigImage image.Image
	Thumbnail image.Image
}

// UploadImages uploads original and thumbnail of the image.
func (e *GCSEnhancer) UploadImages(ctx context.Context, imgs []Images) (SortedLinks, error) {
	ois := make([]*ObjectInfo, 0)
	var (
		err error
		sl  SortedLinks
	)

	for _, img := range imgs {
		// Upload both orginal / thumbnail images.
		origName := AppendUnixTimeStampToFilename(filepath.Base(img.Name))
		thumbnailName := AppendUnixTimeStampToFilename(appendThumbnailStamp(filepath.Base(img.Name)))

		origBuf := new(bytes.Buffer)
		thumbBuf := new(bytes.Buffer)

		var (
			origObj  *ObjectInfo
			thumbObj *ObjectInfo
		)

		switch img.Mime {
		case "image/png":
			enc := png.Encoder{
				CompressionLevel: png.BestCompression,
			}

			if err = enc.Encode(origBuf, img.OrigImage); err != nil {
				return sl, err
			}

			origObj = &ObjectInfo{
				Size:   Original,
				Name:   origName,
				Reader: origBuf,
			}

			if err = enc.Encode(thumbBuf, img.Thumbnail); err != nil {
				return sl, err
			}

			thumbObj = &ObjectInfo{
				Size:   Thumbnail,
				Name:   thumbnailName,
				Reader: thumbBuf,
			}
		case "image/jpeg":
			if err := jpeg.Encode(origBuf, img.OrigImage, &jpeg.Options{
				Quality: jpeg.DefaultQuality,
			}); err != nil {
				return sl, err
			}

			origObj = &ObjectInfo{
				Size:   Original,
				Name:   origName,
				Reader: origBuf,
			}

			if err := jpeg.Encode(thumbBuf, img.Thumbnail, &jpeg.Options{
				Quality: 40,
			}); err != nil {
				return sl, err
			}

			thumbObj = &ObjectInfo{
				Size:   Thumbnail,
				Name:   thumbnailName,
				Reader: thumbBuf,
			}

		case "image/gif":
			if err := gif.Encode(origBuf, img.OrigImage, &gif.Options{}); err != nil {
				return sl, err
			}

			origObj = &ObjectInfo{
				Size:   Original,
				Name:   origName,
				Reader: origBuf,
			}

			if err := gif.Encode(thumbBuf, img.Thumbnail, &gif.Options{}); err != nil {
				return sl, err
			}

			origObj = &ObjectInfo{
				Size:   Thumbnail,
				Name:   origName,
				Reader: origBuf,
			}
		}

		ois = append(ois, origObj)
		ois = append(ois, thumbObj)

	}

	sl, err = e.uploadMultiple(ctx, ois...)

	if err != nil {
		return sl, err
	}

	return sl, err
}

type ImageSize string

const (
	Original  ImageSize = "original"
	Thumbnail ImageSize = "thumbnail"
)

type ObjectInfo struct {
	Size   ImageSize
	Name   string
	Reader io.Reader
}

type SortedLinks struct {
	Thumbnails []string `json:"thumbnails"`
	Original   []string `json:"originals"`
}

func (e *GCSEnhancer) uploadMultiple(ctx context.Context, objs ...*ObjectInfo) (SortedLinks, error) {
	quit := make(chan struct{}, 1)
	errChan := make(chan error, 1)

	type LinkInfo struct {
		Size ImageSize
		Link string
	}

	linkChan := make(chan LinkInfo, 1)
	sl := SortedLinks{}

L:
	for _, obj := range objs {
		select {
		case <-quit:
			break L
		default:
			go func(obj *ObjectInfo) {
				// Test: write to physical file for testing purpose.
				objectLink, err := e.Upload(
					ctx,
					obj.Reader,
					obj.Name,
				)

				if err != nil {
					errChan <- err
					close(quit)

					return
				}

				errChan <- nil
				linkChan <- LinkInfo{
					Size: obj.Size,
					Link: objectLink,
				}

			}(obj)
		}
	}

	for range objs {
		if err := <-errChan; err != nil {
			close(quit)

			return sl, err
		}

		li := <-linkChan

		if li.Size == Original {
			sl.Original = append(sl.Original, li.Link)
		}

		if li.Size == Thumbnail {
			sl.Thumbnails = append(sl.Thumbnails, li.Link)
		}
	}

	b, err := json.Marshal(sl)

	if err != nil {
		return sl, err
	}

	log.Printf("All file uploaded success %s", string(b))

	return sl, nil
}
