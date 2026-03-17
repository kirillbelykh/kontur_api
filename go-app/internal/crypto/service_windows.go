//go:build windows

package crypto

import (
	"context"
	"errors"
	"runtime"
	"strings"
	"time"

	"github.com/go-ole/go-ole"
	"github.com/go-ole/go-ole/oleutil"

	"github.com/kirillbelykh/kontur_api/go-app/internal/config"
	"github.com/kirillbelykh/kontur_api/go-app/internal/dto"
)

type windowsProvider struct{}

func newProvider(cfg config.Config) provider {
	_ = cfg
	return &windowsProvider{}
}

func (w *windowsProvider) FindCertificateThumbprint(ctx context.Context) (string, error) {
	_ = ctx
	return w.withStore(func(certs *ole.IDispatch) (string, error) {
		countVariant, err := oleutil.GetProperty(certs, "Count")
		if err != nil {
			return "", err
		}
		count := int(countVariant.Val)
		for idx := 1; idx <= count; idx++ {
			certVariant, err := oleutil.CallMethod(certs, "Item", idx)
			if err != nil {
				continue
			}
			cert := certVariant.ToIDispatch()
			thumbVariant, err := oleutil.GetProperty(cert, "Thumbprint")
			if err == nil {
				thumbprint := strings.ToLower(strings.TrimSpace(thumbVariant.ToString()))
				cert.Release()
				if thumbprint != "" {
					return thumbprint, nil
				}
				continue
			}
			cert.Release()
		}
		return "", errors.New("no certificate found")
	})
}

func (w *windowsProvider) SignBase64(ctx context.Context, requestedThumbprint, content string, detached bool) (string, error) {
	_ = ctx
	return w.withStore(func(certs *ole.IDispatch) (string, error) {
		countVariant, err := oleutil.GetProperty(certs, "Count")
		if err != nil {
			return "", err
		}
		count := int(countVariant.Val)

		var selected *ole.IDispatch
		for idx := 1; idx <= count; idx++ {
			certVariant, err := oleutil.CallMethod(certs, "Item", idx)
			if err != nil {
				continue
			}
			cert := certVariant.ToIDispatch()
			thumbVariant, err := oleutil.GetProperty(cert, "Thumbprint")
			if err != nil {
				cert.Release()
				continue
			}
			thumbprint := strings.ToLower(strings.TrimSpace(thumbVariant.ToString()))
			if thumbprint == "" {
				cert.Release()
				continue
			}
			if requestedThumbprint == "" || thumbprint == strings.ToLower(strings.TrimSpace(requestedThumbprint)) {
				selected = cert
				break
			}
			cert.Release()
		}
		if selected == nil {
			return "", errors.New("no matching certificate found")
		}
		defer selected.Release()

		signerObj, err := oleutil.CreateObject("CAdESCOM.CPSigner")
		if err != nil {
			return "", err
		}
		defer signerObj.Release()
		signer, err := signerObj.QueryInterface(ole.IID_IDispatch)
		if err != nil {
			return "", err
		}
		defer signer.Release()

		if _, err := oleutil.PutProperty(signer, "Certificate", selected); err != nil {
			return "", err
		}

		attrObj, err := oleutil.CreateObject("CAdESCOM.CPAttribute")
		if err != nil {
			return "", err
		}
		defer attrObj.Release()
		attr, err := attrObj.QueryInterface(ole.IID_IDispatch)
		if err != nil {
			return "", err
		}
		defer attr.Release()

		if _, err := oleutil.PutProperty(attr, "Name", 0); err != nil {
			return "", err
		}
		if _, err := oleutil.PutProperty(attr, "Value", time.Now()); err != nil {
			return "", err
		}

		attrsVariant, err := oleutil.GetProperty(signer, "AuthenticatedAttributes2")
		if err != nil {
			return "", err
		}
		attrs := attrsVariant.ToIDispatch()
		defer attrs.Release()
		if _, err := oleutil.CallMethod(attrs, "Add", attr); err != nil {
			return "", err
		}

		signedDataObj, err := oleutil.CreateObject("CAdESCOM.CadesSignedData")
		if err != nil {
			return "", err
		}
		defer signedDataObj.Release()
		signedData, err := signedDataObj.QueryInterface(ole.IID_IDispatch)
		if err != nil {
			return "", err
		}
		defer signedData.Release()

		if _, err := oleutil.PutProperty(signedData, "ContentEncoding", 1); err != nil {
			return "", err
		}
		if _, err := oleutil.PutProperty(signedData, "Content", content); err != nil {
			return "", err
		}

		signatureVariant, err := oleutil.CallMethod(signedData, "SignCades", signer, 1, detached, 0)
		if err != nil {
			return "", err
		}
		signature := strings.ReplaceAll(signatureVariant.ToString(), "\r", "")
		signature = strings.ReplaceAll(signature, "\n", "")
		return signature, nil
	})
}

func (w *windowsProvider) withStore(run func(certs *ole.IDispatch) (string, error)) (string, error) {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	if err := ole.CoInitialize(0); err != nil {
		return "", err
	}
	defer ole.CoUninitialize()

	storeObj, err := oleutil.CreateObject("CAdESCOM.Store")
	if err != nil {
		return "", err
	}
	defer storeObj.Release()
	store, err := storeObj.QueryInterface(ole.IID_IDispatch)
	if err != nil {
		return "", err
	}
	defer store.Release()

	if _, err := oleutil.CallMethod(store, "Open", 2, "My", 2); err != nil {
		return "", err
	}
	defer oleutil.CallMethod(store, "Close")

	certsVariant, err := oleutil.GetProperty(store, "Certificates")
	if err != nil {
		return "", err
	}
	certs := certsVariant.ToIDispatch()
	defer certs.Release()

	return run(certs)
}

func (w *windowsProvider) State() dto.DependencyStatus {
	return dto.DependencyStatus{
		Name:      "cryptopro",
		Available: true,
		Status:    "windows-com",
		Hint:      "CAdESCOM bridge is available in Windows builds.",
	}
}
