package de.dfki.km.perspecting.obie.transducer.model;

public class LiteralHashing {

	final int characterLength;
	
	public int getCharacterLength() {
		return characterLength;
	}
	
	public LiteralHashing(int characterLength) {
		this.characterLength = characterLength;
	}
	
	public int hash(String input) {
		
		if(input.length() > characterLength)
			return input.substring(0, characterLength).hashCode();
		else 
			return input.hashCode();
	}
	
}
