const { MongoClient } = require("mongodb");
const fs = require("fs");

/* Segundo parcial de la materia Bases de Datos II
 * Realizado por: Rodriguez, Valentin
 * Legajo: 116385
 * DNI: 45302910
 * Fecha: 25/06/2025
*/

const uriOrigen = "mongodb+srv://user:user@332.tnpvcy8.mongodb.net/?retryWrites=true&w=majority&appName=332";
const uriDestino = "mongodb+srv://user:user@parcial3321.zvcub3u.mongodb.net/?retryWrites=true&w=majority&appName=Parcial3321";

const clienteOrigen = new MongoClient(uriOrigen);
const clienteDestino = new MongoClient(uriDestino);

// PARTE 1 //
async function transferirDatos() {
    try {
        // Conexion al cluster de origen
        await clienteOrigen.connect();
        const dbOrigen = clienteOrigen.db("parcial_bd2");
        const coleccionOrigen = dbOrigen.collection("usuarios");

        // Trae todos los usuarios
        const usuariosOrigen = await coleccionOrigen.find();
        const todosLosUsuarios = await usuariosOrigen.toArray();

        // Conexión al cluster de destino
        await clienteDestino.connect();
        const dbDestino = clienteDestino.db("rodriguez_116385");
        const coleccionUsuarios = dbDestino.collection("usuarios_uu");

        for (const usuario of todosLosUsuarios) {

            const usuarioNuevo = {
                _id: usuario._id,
                nombre: usuario.nombre,
                dni: usuario.dni,
                correo: usuario.correo,
                estado: usuario.estado,
                procesado: true
            };

            await coleccionUsuarios.insertOne(usuarioNuevo);
        }
    } catch (error) {
        console.error("Error durante la transferencia de películas:", error);
    } finally {
        await clienteOrigen.close();
        await clienteDestino.close();
    }
}

// PARTE 2 //
//Modelo Uno a Uno 
async function insertarInscripciones() {
    try {
        await clienteDestino.connect();
        const db = clienteDestino.db("rodriguez_116385");
        const usuarios = await db.collection("usuarios_uu").find().limit(10).toArray();

        const cursos = ["Matematica", "Historia", "Fisica", "Quimica", "Lengua", "Biologia", "Geografia", "Arte", "Musica", "Programacion"];

        const inscripciones = usuarios.map((usuario, i) => ({
            curso: cursos[i],
            fecha_inscripcion: new Date(),
            nota_final: Math.floor(Math.random() * 9) + 2,
            usuario_id: usuario._id
        }));

        await db.collection("inscripciones_uu").insertMany(inscripciones);
    } catch (error) {
        console.error("Error al insertar inscripciones:", error);
    } finally {
        await clienteDestino.close();
    }
}

//Modelo Uno a Muchos
async function insertarCursos() {
    try {
        await clienteDestino.connect();
        const db = clienteDestino.db("rodriguez_116385");
        const usuarios = await db.collection("usuarios_uu").find().limit(10).toArray();

        const cursos = [
            { nombre: "Programacion", profesor: "Emanuel Tevez" },
            { nombre: "Historia", profesor: "Juan Perez" },
            { nombre: "Fisica", profesor: "Albert Einstein" },
            { nombre: "Lengua", profesor: "Jhon Doe" },
            { nombre: "Matematica", profesor: "Julieta Lopez" }
        ];

        let usuarioIndex = 0;
        const cursosConInscripciones = cursos.map(curso => {
            const inscripciones = [];

            for (let i = 0; i < 2; i++) {
                if (usuarioIndex >= usuarios.length) usuarioIndex = 0;

                inscripciones.push({
                    usuario_id: usuarios[usuarioIndex]._id,
                    fecha: new Date(),
                    nota: Math.floor(Math.random() * 9) + 2
                });

                usuarioIndex++;
            }

            return {
                ...curso,
                activo: true,
                inscripciones
            };
        });

        await db.collection("cursos_um").insertMany(cursosConInscripciones);
    } catch (error) {
        console.error("Error al insertar cursos_um:", error);
    } finally {
        await clienteDestino.close();
    }
}

//Modleo Muchos a Muchos
async function insertarEstudiantesCursosMm() {
    try {
        await clienteDestino.connect();
        const db = clienteDestino.db("rodriguez_116385");

        const estudiantes = await db.collection("usuarios_uu").find().limit(4).toArray();
        const cursos = await db.collection("cursos_um").find().limit(5).toArray();

        // Insertar estudiantes_mm
        for (let i = 0; i < estudiantes.length; i++) {
            const curso_ids = [
                cursos[i % 5]._id,
                cursos[(i + 1) % 5]._id,
                cursos[(i + 2) % 5]._id
            ];
            await db.collection("estudiantes_mm").insertOne({
                usuario_id: estudiantes[i]._id,
                nombre: estudiantes[i].nombre,
                curso_ids
            });
        }

        // Insertar cursos_mm
        for (let i = 0; i < cursos.length; i++) {
            const estudiante_ids = [
                estudiantes[i % 4]._id,
                estudiantes[(i + 1) % 4]._id
            ];
            await db.collection("cursos_mm").insertOne({
                curso_id: cursos[i]._id,
                nombre: cursos[i].nombre,
                estudiante_ids
            });
        }
    } catch (error) {
        console.error("Error en muchos a muchos:", error);
    } finally {
        await clienteDestino.close();
    }
}

// PARTE 3 //
// CONSULTAS A REALIZAR //
async function consultas(dniAlumno) {
    try {
        await clienteDestino.connect();
        const db = clienteDestino.db("rodriguez_116385");

        // 1. Mostrar todos los cursos a los que está inscripto un alumno usando su DNI(LEGAJO).
        const alumno = await db.collection("usuarios_uu").findOne({ dni: dniAlumno });
        if (!alumno) {
            console.log("Alumno no encontrado");
            return;
        }
        const inscripciones = await db.collection("inscripciones_uu").find({ usuario_id: alumno._id }).toArray();
        const cursosInscripto = inscripciones.map(i => i.curso);
        console.log("1. Cursos inscripto:", cursosInscripto);

        // 2. Mostrar su nota más alta y el curso correspondiente.
        if (inscripciones.length > 0) {
            const mejor = inscripciones.reduce((a, b) => a.nota_final > b.nota_final ? a : b);
            console.log("2. Nota más alta:", mejor.nota_final, "en", mejor.curso);
        }

        // 3. Listar cursos donde la inscripción fue posterior al 16/06/2025. (de todos los alumnos)
        const fechaLimite = new Date("2025-06-16");
        const inscripcionesPosteriores = await db.collection("inscripciones_uu").find({ fecha_inscripcion: { $gt: fechaLimite } }).toArray();
        const cursosPosteriores = [...new Set(inscripcionesPosteriores.map(i => i.curso))];
        console.log("3. Cursos con inscripción posterior al 16/06/2025:", cursosPosteriores);

        // 4. Mostrar cursos activos donde ud este inscripto.
        const cursosActivos = await db.collection("cursos_um").find({ 
            activo: true, 
            "inscripciones.usuario_id": alumno._id 
        }).toArray();
        console.log("4. Cursos activos inscripto:", cursosActivos.map(c => c.nombre));

        // 5. Nombre del profesor y fecha de inscripción de cada curso del alumno.
        for (const insc of inscripciones) {
            const curso = await db.collection("cursos_um").findOne({ nombre: insc.curso });
            if (curso) {
                console.log(`5. Profesor: ${curso.profesor}, Fecha inscripción: ${insc.fecha_inscripcion.toLocaleDateString()}`);
            }
        }

        // 6. Nombre completo del alumno, nombre del curso, estado (activo o inactivo) y nota final.
        for (const insc of inscripciones) {
            const curso = await db.collection("cursos_um").findOne({ nombre: insc.curso });
            if (curso) {
                console.log(`6. Alumno: ${alumno.nombre}, Curso: ${curso.nombre}, Estado: ${curso.activo ? "activo" : "inactivo"}, Nota: ${insc.nota_final}`);
            }
        }

        // 7. Calcular el promedio de notas del alumno.
        if (inscripciones.length > 0) {
            const promedio = inscripciones.reduce((sum, i) => sum + i.nota_final, 0) / inscripciones.length;
            console.log("7. Promedio de notas:", promedio.toFixed(2));
        }

        // 8. Mostrar la cantidad de inscripciones por curso.
        const agg = await db.collection("inscripciones_uu").aggregate([
            { $group: { _id: "$curso", cantidad: { $sum: 1 } } }
        ]).toArray();
        console.log("8. Cantidad de inscripciones por curso:", agg);

        // 9. Estudiantes con estado "activo" en el curso "MongoDB Avanzado".
        const cursoMongo = await db.collection("cursos_um").findOne({ nombre: "MongoDB Avanzado" });
        if (cursoMongo) {
            const inscMongo = cursoMongo.inscripciones || [];
            for (const insc of inscMongo) {
                const est = await db.collection("usuarios_uu").findOne({ _id: insc.usuario_id, estado: "activo" });
                if (est) {
                    console.log(`9. Estudiante activo en MongoDB Avanzado: ${est.nombre}`);
                }
            }
        } else {
            console.log("9. No existe el curso MongoDB Avanzado.");
        }

        // 10. Cursos inactivos con sus estudiantes.
        const cursosInactivos = await db.collection("cursos_um").find({ activo: false }).toArray();
        for (const curso of cursosInactivos) {
            const estudiantes = [];
            for (const insc of (curso.inscripciones || [])) {
                const est = await db.collection("usuarios_uu").findOne({ _id: insc.usuario_id });
                if (est) estudiantes.push(est.nombre);
            }
            console.log(`10. Curso inactivo: ${curso.nombre}, Estudiantes: ${estudiantes.join(", ")}`);
        }

    } catch (error) {
        console.error("Error en consultas:", error);
    } finally {
        await clienteDestino.close();
    }
}

// EXPORTAR COLLECIONES A JSON //
async function exportarJson() {
    try {
        await clienteDestino.connect();
        const db = clienteDestino.db("rodriguez_116385");

        // Lista de colecciones y nombres de archivo
        const colecciones = [
            { nombre: "usuarios_uu", archivo: "usuarios_uu.json" },
            { nombre: "inscripciones_uu", archivo: "inscripciones_uu.json" },
            { nombre: "cursos_um", archivo: "cursos_um.json" },
            { nombre: "estudiantes_mm", archivo: "estudiantes_mm.json" },
            { nombre: "cursos_mm", archivo: "cursos_mm.json" }
        ];

        for (const col of colecciones) {
            const datos = await db.collection(col.nombre).find().toArray();
            fs.writeFileSync(col.archivo, JSON.stringify(datos, null, 2));
            console.log(`Exportacion completada: ${col.archivo}`);
        }
    } catch (error) {
        console.error("Error exportando colecciones:", error);
    } finally {
        await clienteDestino.close();
    }
}

// Para ejecutar la exportacion de usuarios:
transferirDatos();

// Para ejecutar la insercion de inscripciones:
insertarInscripciones();

// Para ejecutar la insercion de cursos:
insertarCursos();

// Para ejecutar la insercion muchos a muchos:
insertarEstudiantesCursosMm();

// Para ejecutar consultas:
consultas("37809"); // Reemplaza por el DNI que quieras consultar

// Para ejecutar la generacion de archivos JSON:
exportarJson();