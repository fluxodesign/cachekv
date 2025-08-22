package cachekv

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/x509"
	"encoding/hex"
	"encoding/pem"
	"errors"
	"io"
	"log"
	mrand "math/rand"
	"os"
	"path"
	"strconv"
	"time"

	"github.com/foundriesio/go-ecies"
)

var (
	privateFile = "key.pem"
	publicFile  = "public.pem"
)

func genKeypair() error {
	// WARNING: this will overwrite existing keypair
	curve := elliptic.P384()
	private, err := ecdsa.GenerateKey(curve, rand.Reader)
	if err != nil {
		log.Printf("Error generating keypair: %v", err)
		return err
	}
	public := &private.PublicKey
	strPrivate, strPublic := encode(private, public)
	err = writeToStorage(strPrivate, strPublic, KeyPath, true)
	return err
}

func encode(privateKey *ecdsa.PrivateKey, publicKey *ecdsa.PublicKey) ([]byte, []byte) {
	x509Encoded, _ := x509.MarshalECPrivateKey(privateKey)
	pemEncoded := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: x509Encoded})
	x509EncodePub, _ := x509.MarshalPKIXPublicKey(publicKey)
	pemEncodedPub := pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: x509EncodePub})
	return pemEncoded, pemEncodedPub
}

func decode(pemEncoded []byte, pemEncodePub []byte) (*ecdsa.PrivateKey, *ecdsa.PublicKey) {
	bytes, _ := pem.Decode(pemEncoded)
	x509Encoded := bytes.Bytes
	privateKey, _ := x509.ParseECPrivateKey(x509Encoded)

	bytesPub, _ := pem.Decode(pemEncodePub)
	x509EncodedPub := bytesPub.Bytes
	genericPublicKey, _ := x509.ParsePKIXPublicKey(x509EncodedPub)
	publicKey := genericPublicKey.(*ecdsa.PublicKey)
	return privateKey, publicKey
}

func writeToStorage(privateKey []byte, publicKey []byte, targetDir string, overwrite ...bool) error {
	privatePath := path.Join(targetDir, privateFile)
	publicPath := path.Join(targetDir, publicFile)
	shouldOverwrite := false
	if len(overwrite) > 0 {
		shouldOverwrite = overwrite[0]
	}
	if _, err := os.Stat(targetDir); os.IsNotExist(err) {
		err = os.MkdirAll(targetDir, 0744)
		if err != nil {
			return err
		}
	}
	_, errPrivate := os.Stat(privatePath)
	_, errPublic := os.Stat(publicPath)
	if errPrivate == nil || errPublic == nil {
		if !shouldOverwrite {
			return errors.New("target file(s) already exists")
		} else {
			// not really overwriting file, rename
			oldPath := privatePath
			newPath := privatePath + "." + strconv.FormatInt(time.Now().Unix(), 10)
			err := os.Rename(oldPath, newPath)
			if err != nil {
				return err
			}
			oldPath = publicPath
			newPath = publicPath + "." + strconv.FormatInt(time.Now().Unix(), 10)
			err = os.Rename(oldPath, newPath)
			if err != nil {
				return err
			}
		}
	}
	err := os.WriteFile(privatePath, privateKey, 0600)
	if err != nil {
		return err
	}
	err = os.WriteFile(publicPath, publicKey, 0644)
	return err
}

func readFromStorage(targetDir string) (*ecdsa.PrivateKey, *ecdsa.PublicKey, error) {
	privatePath := path.Join(targetDir, privateFile)
	if _, err := os.Stat(privatePath); os.IsNotExist(err) {
		return nil, nil, errors.New("private key does not exist")
	}
	publicPath := path.Join(targetDir, publicFile)
	if _, err := os.Stat(publicPath); os.IsNotExist(err) {
		return nil, nil, errors.New("public key does not exist")
	}
	privateBytes, err := os.ReadFile(privatePath)
	if err != nil {
		return nil, nil, err
	}
	publicBytes, err := os.ReadFile(publicPath)
	if err != nil {
		return nil, nil, err
	}
	privKey, pubKey := decode(privateBytes, publicBytes)
	return privKey, pubKey, nil
}

func encryptMessage(message []byte, shared []byte) ([]byte, error) {
	_, ecdsaPub, err := readFromStorage(KeyPath)
	if err != nil {
		return nil, err
	}
	// import the keys as ECIES keys
	eciesPublic := ecies.ImportECDSAPublic(ecdsaPub)
	encrypted, err := ecies.Encrypt(rand.Reader, eciesPublic, message, shared, nil)
	return encrypted, err
}

func decryptMessage(encrypted []byte, shared []byte) ([]byte, error) {
	ecdsaPriv, _, err := readFromStorage(KeyPath)
	if err != nil {
		return nil, err
	}
	eciesPrivate := ecies.ImportECDSA(ecdsaPriv)
	decrypted, err := ecies.Decrypt(eciesPrivate, encrypted, shared, nil)
	return decrypted, err
}

func hashFile(path string) (string, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer func(file *os.File) {
		err := file.Close()
		if err != nil {
			log.Printf("Error closing file: %v", err)
		}
	}(file)
	hash := sha256.New()
	if _, err := io.Copy(hash, file); err != nil {
		return "", err
	}
	byteHash := hash.Sum(nil)
	strHash := hex.EncodeToString(byteHash)
	return strHash, nil
}

func extractString(source string, intendedLength int) (string, error) {
	var paddingChars = []rune("#$%&^*0@!")
	if intendedLength <= 0 {
		return "", errors.New("invalid intended length, you are asking for the impossible")
	}
	if len(source) < intendedLength {
		padding := make([]rune, intendedLength-len(source))
		for i := range padding {
			padding[i] = paddingChars[mrand.Intn(len(paddingChars))]
		}
		return source + string(padding), nil
	}
	return source[:intendedLength], nil
}
