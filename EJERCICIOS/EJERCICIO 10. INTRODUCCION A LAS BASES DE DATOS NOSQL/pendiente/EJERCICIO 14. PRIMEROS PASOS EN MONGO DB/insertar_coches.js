// Comando para crear la colección "coches"
// db.createCollection("coches")

// Documentos de ejemplo para insertar en la colección "coches"
var coches10 = [
    {
        marca: "Toyota",
        modelo: "Corolla",
        color: "Rojo",
        matricula: "ABC123",
        precio: 25000
    },
    {
        marca: "Ford",
        modelo: "Mustang",
        color: "Azul",
        matricula: "DEF456",
        precio: 45000
    },
    {
        marca: "Chevrolet",
        modelo: "Camaro",
        color: "Negro",
        matricula: "GHI789",
        precio: 42000
    },
    {
        marca: "Honda",
        modelo: "Civic",
        color: "Blanco",
        matricula: "JKL012",
        precio: 22000
    },
    {
        marca: "Nissan",
        modelo: "Altima",
        color: "Plata",
        matricula: "MNO345",
        precio: 28000
    },
    {
        marca: "Volkswagen",
        modelo: "Golf",
        color: "Verde",
        matricula: "PQR678",
        precio: 20000
    },
    {
        marca: "BMW",
        modelo: "X5",
        color: "Gris",
        matricula: "STU901",
        precio: 55000
    },
    {
        marca: "Mercedes-Benz",
        modelo: "C-Class",
        color: "Plateado",
        matricula: "VWX234",
        precio: 50000
    },
    {
        marca: "Audi",
        modelo: "A4",
        color: "Azul Oscuro",
        matricula: "YZA567",
        precio: 48000
    },
    {
        marca: "Kia",
        modelo: "Sorento",
        color: "Marrón",
        matricula: "BCD890",
        precio: 30000
    }
]


// Insertar los documentos en la colección "coches"
db.coches.insertMany(coches10)
