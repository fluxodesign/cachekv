package cachekv

import (
	"log"
	"os"
	"path"
	"testing"

	"github.com/stretchr/testify/assert"
)

var (
	alternateDir = "./test-alternate"
)

func cipherSetup() func() {
	var err error
	privateDir = alternateDir
	return func() {
		err = os.RemoveAll(alternateDir)
		if err != nil {
			log.Println("error removing test alternate directory")
		}
	}
}

func TestGenKeypair(t *testing.T) {
	defer cipherSetup()()
	privatePath := path.Join(alternateDir, privateFile)
	publicPath := path.Join(alternateDir, publicFile)
	assert.Nil(t, genKeypair())
	info, err := os.Stat(privatePath)
	assert.Nil(t, err)
	fMode := info.Mode()
	assert.True(t, fMode&os.FileMode(0600) != 0)
	info, err = os.Stat(publicPath)
	assert.Nil(t, err)
	fMode = info.Mode()
	assert.True(t, fMode&os.FileMode(0644) != 0)
}

func TestGenKeypairAlreadyExists(t *testing.T) {
	defer cipherSetup()()
	privatePath := path.Join(alternateDir, privateFile)
	publicPath := path.Join(alternateDir, publicFile)
	assert.Nil(t, genKeypair())
	_, err := os.Stat(privatePath)
	assert.Nil(t, err)
	_, err = os.Stat(publicPath)
	assert.Nil(t, err)
	// second try
	assert.NotNil(t, genKeypair())
}

func TestCheckPrivateAndPublicKeys(t *testing.T) {
	defer cipherSetup()()
	assert.Nil(t, genKeypair())
	privateKey, publicKey, err := readFromStorage(alternateDir)
	assert.Nil(t, err)
	assert.NotNil(t, privateKey)
	assert.NotNil(t, publicKey)
	assert.True(t, privateKey.PublicKey.Equal(publicKey))
}

func TestEncryptDecryptMessage(t *testing.T) {
	defer cipherSetup()()
	assert.Nil(t, genKeypair())
	sharedValue := []byte("34534532|34t4645")
	message := []byte("Hello,World! This is me!")
	encrypted, err := encryptMessage(message, sharedValue)
	assert.Nil(t, err)
	decrypted, err := decryptMessage(encrypted, sharedValue)
	assert.Nil(t, err)
	assert.Equal(t, message, decrypted)
}
